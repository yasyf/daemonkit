//go:build darwin && !daemonkit_unsigned

package trust

import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/yasyf/daemonkit/wire"
)

// csRuntime is the CS_RUNTIME code-signing status bit: the peer's binary opted
// into the Hardened Runtime (codesign --options runtime).
const csRuntime = 0x00010000

// kCFStringEncodingUTF8 is CoreFoundation's UTF-8 encoding constant.
const kCFStringEncodingUTF8 = 0x08000100

// errSecSuccess is OSStatus 0; every nonzero status the verifier sees is untrusted.
const errSecSuccess = 0

var (
	secOnce sync.Once
	secErr  error

	cfDataCreate                   func(alloc uintptr, bytes *byte, length int) uintptr
	cfStringCreateWithCString      func(alloc uintptr, cstr string, enc uint32) uintptr
	cfDictionaryCreate             func(alloc uintptr, keys, values *uintptr, num int, keyCB, valCB uintptr) uintptr
	cfNumberGetValue               func(number uintptr, theType int, valuePtr *int64) bool
	cfDictionaryGetValue           func(dict, key uintptr) uintptr
	cfRelease                      func(cf uintptr)
	secCodeCopyGuestWithAttributes func(host, attrs uintptr, flags uint32, guest *uintptr) int32
	secRequirementCreateWithString func(text uintptr, flags uint32, req *uintptr) int32
	secCodeCheckValidityWithErrors func(code uintptr, flags uint32, req uintptr, errs *uintptr) int32
	secCodeCopySigningInformation  func(code uintptr, flags uint32, info *uintptr) int32

	auditAttrKey  uintptr // kSecGuestAttributeAudit (a CFStringRef)
	infoStatusKey uintptr // kSecCodeInfoStatus (a CFStringRef)
	dictKeyCB     uintptr // &kCFTypeDictionaryKeyCallBacks
	dictValCB     uintptr // &kCFTypeDictionaryValueCallBacks
)

func loadSecurity() {
	cf, err := purego.Dlopen("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		secErr = fmt.Errorf("trust: dlopen CoreFoundation: %w", err)
		return
	}
	sec, err := purego.Dlopen("/System/Library/Frameworks/Security.framework/Security", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		secErr = fmt.Errorf("trust: dlopen Security: %w", err)
		return
	}
	purego.RegisterLibFunc(&cfDataCreate, cf, "CFDataCreate")
	purego.RegisterLibFunc(&cfStringCreateWithCString, cf, "CFStringCreateWithCString")
	purego.RegisterLibFunc(&cfDictionaryCreate, cf, "CFDictionaryCreate")
	purego.RegisterLibFunc(&cfNumberGetValue, cf, "CFNumberGetValue")
	purego.RegisterLibFunc(&cfDictionaryGetValue, cf, "CFDictionaryGetValue")
	purego.RegisterLibFunc(&cfRelease, cf, "CFRelease")
	purego.RegisterLibFunc(&secCodeCopyGuestWithAttributes, sec, "SecCodeCopyGuestWithAttributes")
	purego.RegisterLibFunc(&secRequirementCreateWithString, sec, "SecRequirementCreateWithString")
	purego.RegisterLibFunc(&secCodeCheckValidityWithErrors, sec, "SecCodeCheckValidityWithErrors")
	purego.RegisterLibFunc(&secCodeCopySigningInformation, sec, "SecCodeCopySigningInformation")

	// kSecGuestAttributeAudit / kSecCodeInfoStatus are CFStringRef DATA symbols:
	// dlsym returns the symbol's address, which must be dereferenced. The
	// dictionary callbacks are STRUCTs: the dlsym address is passed directly.
	var derefErr error
	auditAttrKey, derefErr = derefStringSym(sec, "kSecGuestAttributeAudit")
	if derefErr != nil {
		secErr = derefErr
		return
	}
	infoStatusKey, derefErr = derefStringSym(sec, "kSecCodeInfoStatus")
	if derefErr != nil {
		secErr = derefErr
		return
	}
	if dictKeyCB, derefErr = purego.Dlsym(cf, "kCFTypeDictionaryKeyCallBacks"); derefErr != nil {
		secErr = fmt.Errorf("trust: dlsym key callbacks: %w", derefErr)
		return
	}
	if dictValCB, derefErr = purego.Dlsym(cf, "kCFTypeDictionaryValueCallBacks"); derefErr != nil {
		secErr = fmt.Errorf("trust: dlsym value callbacks: %w", derefErr)
		return
	}
}

func derefStringSym(lib uintptr, name string) (uintptr, error) {
	sym, err := purego.Dlsym(lib, name)
	if err != nil {
		return 0, fmt.Errorf("trust: dlsym %s: %w", name, err)
	}
	// A CFStringRef data symbol's value sits at its dlsym address; the address is
	// a fixed dyld location, not a Go pointer (unsafeptr is disabled for FFI).
	return *(*uintptr)(unsafe.Pointer(sym)), nil //nolint:gosec // G103: dereferencing a fixed dlsym data-symbol address
}

// verifyRequirement resolves the peer's SecCode from its audit token, checks it
// against req's designated requirement, and (unless AllowUnhardened) requires the
// Hardened Runtime. Any failure is ErrUntrustedPeer; a missing token or verifier
// is ErrNoVerifier (fail closed).
func verifyRequirement(peer wire.Peer, req Requirement) error {
	if len(peer.Audit) != 32 {
		return fmt.Errorf("%w: audit token is %d bytes, want 32", ErrNoVerifier, len(peer.Audit))
	}
	secOnce.Do(loadSecurity)
	if secErr != nil {
		return fmt.Errorf("%w: %w", ErrNoVerifier, secErr)
	}
	dr, err := req.DRString()
	if err != nil {
		return err
	}

	guest, err := copyGuest(peer.Audit)
	if err != nil {
		return err
	}
	defer cfRelease(guest)

	if err := checkValidity(guest, dr); err != nil {
		return err
	}
	if req.AllowUnhardened {
		return nil
	}
	return requireHardenedRuntime(guest)
}

// copyGuest builds a CFData over the audit token and resolves it to the peer's
// SecCode. The token buffer is pinned only across CFDataCreate, which copies it.
func copyGuest(token []byte) (uintptr, error) {
	var pin runtime.Pinner
	pin.Pin(&token[0])
	cfData := cfDataCreate(0, &token[0], len(token))
	pin.Unpin()
	if cfData == 0 {
		return 0, fmt.Errorf("%w: CFDataCreate returned null", ErrNoVerifier)
	}
	defer cfRelease(cfData)

	keys := []uintptr{auditAttrKey}
	vals := []uintptr{cfData}
	var kp runtime.Pinner
	kp.Pin(&keys[0])
	kp.Pin(&vals[0])
	dict := cfDictionaryCreate(0, &keys[0], &vals[0], 1, dictKeyCB, dictValCB)
	kp.Unpin()
	if dict == 0 {
		return 0, fmt.Errorf("%w: CFDictionaryCreate returned null", ErrNoVerifier)
	}
	defer cfRelease(dict)

	var guest uintptr
	if st := secCodeCopyGuestWithAttributes(0, dict, 0, &guest); st != errSecSuccess {
		return 0, fmt.Errorf("%w: SecCodeCopyGuestWithAttributes: OSStatus %d", ErrNoVerifier, st)
	}
	return guest, nil
}

// checkValidity evaluates the designated requirement against the guest SecCode.
func checkValidity(guest uintptr, dr string) error {
	reqCF := cfStringCreateWithCString(0, dr+"\x00", kCFStringEncodingUTF8)
	if reqCF == 0 {
		return fmt.Errorf("%w: CFStringCreateWithCString returned null", ErrNoVerifier)
	}
	defer cfRelease(reqCF)

	var requirement uintptr
	if st := secRequirementCreateWithString(reqCF, 0, &requirement); st != errSecSuccess {
		return fmt.Errorf("%w: SecRequirementCreateWithString: OSStatus %d", ErrNoVerifier, st)
	}
	defer cfRelease(requirement)

	var cfErr uintptr
	st := secCodeCheckValidityWithErrors(guest, 0, requirement, &cfErr)
	if cfErr != 0 {
		cfRelease(cfErr)
	}
	if st != errSecSuccess {
		return fmt.Errorf("%w: designated requirement not met (OSStatus %d)", ErrUntrustedPeer, st)
	}
	return nil
}

// requireHardenedRuntime rejects a guest whose dynamic signing status lacks the
// CS_RUNTIME bit — a statically valid but injection-permissive peer.
func requireHardenedRuntime(guest uintptr) error {
	const kSecCSDynamicInformation = 1 << 3
	const kCFNumberSInt64Type = 4
	var info uintptr
	if st := secCodeCopySigningInformation(guest, kSecCSDynamicInformation, &info); st != errSecSuccess || info == 0 {
		return fmt.Errorf("%w: SecCodeCopySigningInformation: OSStatus %d", ErrNoVerifier, st)
	}
	defer cfRelease(info)

	statusNum := cfDictionaryGetValue(info, infoStatusKey)
	if statusNum == 0 {
		return fmt.Errorf("%w: peer reports no code-signing status", ErrUntrustedPeer)
	}
	var status int64
	if !cfNumberGetValue(statusNum, kCFNumberSInt64Type, &status) {
		return fmt.Errorf("%w: unreadable code-signing status", ErrNoVerifier)
	}
	if status&csRuntime == 0 {
		return fmt.Errorf("%w: peer lacks the Hardened Runtime (status 0x%x)", ErrUntrustedPeer, status)
	}
	return nil
}
