//go:build darwin && !daemonkit_unsigned

package codeidentity

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"

	"github.com/ebitengine/purego"
	peer "github.com/yasyf/daemonkit/peer"
)

const (
	csGetTaskAllow = 0x00000004
	csForcedLV     = 0x00000010
	csRequireLV    = 0x00002000
	csRuntime      = 0x00010000
	csDebugged     = 0x10000000

	kCFStringEncodingUTF8 = 0x08000100
	errSecSuccess         = 0
)

var (
	secOnce sync.Once
	secErr  error

	cfDataCreate                   func(alloc uintptr, bytes *byte, length int) uintptr
	cfStringCreateWithCString      func(alloc uintptr, cstr string, enc uint32) uintptr
	cfDictionaryCreate             func(alloc uintptr, keys, values *uintptr, num int, keyCB, valCB uintptr) uintptr
	cfNumberGetValue               func(number uintptr, theType int, valuePtr *int64) bool
	cfDictionaryGetValue           func(dict, key uintptr) uintptr
	cfRelease                      func(cf uintptr)
	memoryCopy                     func(destination *uintptr, source uintptr, size uintptr) uintptr
	secCodeCopyGuestWithAttributes func(host, attrs uintptr, flags uint32, guest *uintptr) int32
	secRequirementCreateWithString func(text uintptr, flags uint32, req *uintptr) int32
	secCodeCheckValidityWithErrors func(code uintptr, flags uint32, req uintptr, errs *uintptr) int32
	secCodeCopySigningInformation  func(code uintptr, flags uint32, info *uintptr) int32

	auditAttrKey  uintptr
	infoStatusKey uintptr
	dictKeyCB     uintptr
	dictValCB     uintptr
)

func loadSecurity() {
	cf, err := purego.Dlopen("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		secErr = fmt.Errorf("codeidentity: dlopen CoreFoundation: %w", err)
		return
	}
	sec, err := purego.Dlopen("/System/Library/Frameworks/Security.framework/Security", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		secErr = fmt.Errorf("codeidentity: dlopen Security: %w", err)
		return
	}
	libSystem, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		secErr = fmt.Errorf("codeidentity: dlopen libSystem: %w", err)
		return
	}
	for _, fn := range []struct {
		target any
		lib    uintptr
		name   string
	}{
		{&cfDataCreate, cf, "CFDataCreate"},
		{&cfStringCreateWithCString, cf, "CFStringCreateWithCString"},
		{&cfDictionaryCreate, cf, "CFDictionaryCreate"},
		{&cfNumberGetValue, cf, "CFNumberGetValue"},
		{&cfDictionaryGetValue, cf, "CFDictionaryGetValue"},
		{&cfRelease, cf, "CFRelease"},
		{&memoryCopy, libSystem, "memcpy"},
		{&secCodeCopyGuestWithAttributes, sec, "SecCodeCopyGuestWithAttributes"},
		{&secRequirementCreateWithString, sec, "SecRequirementCreateWithString"},
		{&secCodeCheckValidityWithErrors, sec, "SecCodeCheckValidityWithErrors"},
		{&secCodeCopySigningInformation, sec, "SecCodeCopySigningInformation"},
	} {
		if _, err := purego.Dlsym(fn.lib, fn.name); err != nil {
			secErr = fmt.Errorf("codeidentity: dlsym %s: %w", fn.name, err)
			return
		}
		purego.RegisterLibFunc(fn.target, fn.lib, fn.name)
	}

	auditAttrKey, err = derefStringSym(sec, "kSecGuestAttributeAudit")
	if err != nil {
		secErr = err
		return
	}
	infoStatusKey, err = derefStringSym(sec, "kSecCodeInfoStatus")
	if err != nil {
		secErr = err
		return
	}
	if dictKeyCB, err = purego.Dlsym(cf, "kCFTypeDictionaryKeyCallBacks"); err != nil {
		secErr = fmt.Errorf("codeidentity: dlsym key callbacks: %w", err)
		return
	}
	if dictValCB, err = purego.Dlsym(cf, "kCFTypeDictionaryValueCallBacks"); err != nil {
		secErr = fmt.Errorf("codeidentity: dlsym value callbacks: %w", err)
	}
}

func derefStringSym(lib uintptr, name string) (uintptr, error) {
	sym, err := purego.Dlsym(lib, name)
	if err != nil {
		return 0, fmt.Errorf("codeidentity: dlsym %s: %w", name, err)
	}
	var value uintptr
	memoryCopy(&value, sym, uintptr(strconv.IntSize/8))
	return value, nil
}

func verifyCodeIdentity(peer peer.Identity, identity CodeIdentity) error {
	if len(peer.Audit) != 32 {
		return fmt.Errorf("%w: audit token is %d bytes, want 32", ErrNoVerifier, len(peer.Audit))
	}
	secOnce.Do(loadSecurity)
	if secErr != nil {
		return fmt.Errorf("%w: %w", ErrNoVerifier, secErr)
	}
	dr, err := identity.DRString()
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
	return requireCodePosture(guest)
}

func copyGuest(token []byte) (uintptr, error) {
	var tokenPin runtime.Pinner
	tokenPin.Pin(&token[0])
	cfData := cfDataCreate(0, &token[0], len(token))
	tokenPin.Unpin()
	if cfData == 0 {
		return 0, fmt.Errorf("%w: CFDataCreate returned null", ErrNoVerifier)
	}
	defer cfRelease(cfData)

	keys := []uintptr{auditAttrKey}
	values := []uintptr{cfData}
	var dictionaryPin runtime.Pinner
	dictionaryPin.Pin(&keys[0])
	dictionaryPin.Pin(&values[0])
	dictionary := cfDictionaryCreate(0, &keys[0], &values[0], 1, dictKeyCB, dictValCB)
	dictionaryPin.Unpin()
	if dictionary == 0 {
		return 0, fmt.Errorf("%w: CFDictionaryCreate returned null", ErrNoVerifier)
	}
	defer cfRelease(dictionary)

	var guest uintptr
	if status := secCodeCopyGuestWithAttributes(0, dictionary, 0, &guest); status != errSecSuccess {
		return 0, fmt.Errorf("%w: SecCodeCopyGuestWithAttributes: OSStatus %d", ErrNoVerifier, status)
	}
	return guest, nil
}

func checkValidity(guest uintptr, dr string) error {
	requirementString := cfStringCreateWithCString(0, dr+"\x00", kCFStringEncodingUTF8)
	if requirementString == 0 {
		return fmt.Errorf("%w: CFStringCreateWithCString returned null", ErrNoVerifier)
	}
	defer cfRelease(requirementString)

	var requirement uintptr
	if status := secRequirementCreateWithString(requirementString, 0, &requirement); status != errSecSuccess {
		return fmt.Errorf("%w: SecRequirementCreateWithString: OSStatus %d", ErrNoVerifier, status)
	}
	defer cfRelease(requirement)

	var cfError uintptr
	status := secCodeCheckValidityWithErrors(guest, 0, requirement, &cfError)
	if cfError != 0 {
		cfRelease(cfError)
	}
	if status != errSecSuccess {
		return fmt.Errorf("%w: designated requirement not met (OSStatus %d)", ErrUntrustedPeer, status)
	}
	return nil
}

func requireCodePosture(guest uintptr) error {
	const kSecCSDynamicInformation = 1 << 3
	const kCFNumberSInt64Type = 4
	var info uintptr
	status := secCodeCopySigningInformation(guest, kSecCSDynamicInformation, &info)
	if status != errSecSuccess || info == 0 {
		return fmt.Errorf("%w: SecCodeCopySigningInformation: OSStatus %d", ErrNoVerifier, status)
	}
	defer cfRelease(info)

	statusNumber := cfDictionaryGetValue(info, infoStatusKey)
	if statusNumber == 0 {
		return fmt.Errorf("%w: peer reports no code-signing status", ErrUntrustedPeer)
	}
	var flags int64
	if !cfNumberGetValue(statusNumber, kCFNumberSInt64Type, &flags) {
		return fmt.Errorf("%w: unreadable code-signing status", ErrNoVerifier)
	}
	return checkCodeStatus(flags)
}

func checkCodeStatus(flags int64) error {
	if flags&csRuntime == 0 {
		return fmt.Errorf("%w: peer lacks the Hardened Runtime (status 0x%x)", ErrUntrustedPeer, flags)
	}
	if flags&csGetTaskAllow != 0 {
		return fmt.Errorf("%w: peer permits debugger attachment (CS_GET_TASK_ALLOW, status 0x%x)", ErrUntrustedPeer, flags)
	}
	if flags&csDebugged != 0 {
		return fmt.Errorf("%w: peer ran under a debugger (CS_DEBUGGED, status 0x%x)", ErrUntrustedPeer, flags)
	}
	if flags&(csRequireLV|csForcedLV) == 0 {
		return fmt.Errorf("%w: peer does not enforce library validation (status 0x%x)", ErrUntrustedPeer, flags)
	}
	return nil
}
