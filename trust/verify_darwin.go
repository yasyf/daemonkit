//go:build darwin && !daemonkit_unsigned

package trust

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"sync"

	"github.com/ebitengine/purego"
	"github.com/yasyf/daemonkit/wire"
)

// Dynamic code-signing status bits, verified against xnu's cs_blobs.h.
const (
	csGetTaskAllow = 0x00000004 // CS_GET_TASK_ALLOW: get-task-allow entitlement
	csForcedLV     = 0x00000010 // CS_FORCED_LV: library validation forced by system policy
	csRequireLV    = 0x00002000 // CS_REQUIRE_LV: library validation required
	csRuntime      = 0x00010000 // CS_RUNTIME: Hardened Runtime (codesign --options runtime)
	csDebugged     = 0x10000000 // CS_DEBUGGED: ran with invalid pages under a debugger
)

// entDisableLV turns off library validation unless CS_REQUIRE_LV/CS_FORCED_LV
// enforce it dynamically anyway.
const entDisableLV = "com.apple.security.cs.disable-library-validation"

// injectionEntitlements re-open code injection or debugger attachment on a
// Hardened Runtime binary; a peer signed with any of them is untrusted.
var injectionEntitlements = []string{
	entDisableLV,
	"com.apple.security.cs.allow-dyld-environment-variables",
	"com.apple.security.cs.allow-unsigned-executable-memory",
	"com.apple.security.cs.allow-jit",
	"com.apple.security.cs.disable-executable-page-protection",
	"com.apple.security.get-task-allow",
}

const kCFStringEncodingUTF8 = 0x08000100

const errSecSuccess = 0

var (
	secOnce sync.Once
	secErr  error

	cfDataCreate                   func(alloc uintptr, bytes *byte, length int) uintptr
	cfStringCreateWithCString      func(alloc uintptr, cstr string, enc uint32) uintptr
	cfDictionaryCreate             func(alloc uintptr, keys, values *uintptr, num int, keyCB, valCB uintptr) uintptr
	cfNumberGetValue               func(number uintptr, theType int, valuePtr *int64) bool
	cfDictionaryGetValue           func(dict, key uintptr) uintptr
	cfGetTypeID                    func(cf uintptr) uintptr
	cfBooleanGetTypeID             func() uintptr
	cfStringGetTypeID              func() uintptr
	cfArrayGetTypeID               func() uintptr
	cfDictionaryGetTypeID          func() uintptr
	cfEqual                        func(a, b uintptr) bool
	cfArrayGetCount                func(array uintptr) int
	cfArrayGetValueAtIndex         func(array uintptr, index int) uintptr
	cfRelease                      func(cf uintptr)
	memoryCopy                     func(destination *uintptr, source uintptr, size uintptr) uintptr
	secCodeCopyGuestWithAttributes func(host, attrs uintptr, flags uint32, guest *uintptr) int32
	secRequirementCreateWithString func(text uintptr, flags uint32, req *uintptr) int32
	secCodeCheckValidityWithErrors func(code uintptr, flags uint32, req uintptr, errs *uintptr) int32
	secCodeCopySigningInformation  func(code uintptr, flags uint32, info *uintptr) int32

	auditAttrKey    uintptr
	infoStatusKey   uintptr
	infoEntsDictKey uintptr
	infoEntsKey     uintptr
	cfBooleanFalse  uintptr
	cfBooleanTrue   uintptr
	dictKeyCB       uintptr
	dictValCB       uintptr
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
	libSystem, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		secErr = fmt.Errorf("trust: dlopen libSystem: %w", err)
		return
	}
	// RegisterLibFunc panics on a missing symbol; probe with Dlsym first so framework skew fails closed through secErr.
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
		{&cfGetTypeID, cf, "CFGetTypeID"},
		{&cfBooleanGetTypeID, cf, "CFBooleanGetTypeID"},
		{&cfStringGetTypeID, cf, "CFStringGetTypeID"},
		{&cfArrayGetTypeID, cf, "CFArrayGetTypeID"},
		{&cfDictionaryGetTypeID, cf, "CFDictionaryGetTypeID"},
		{&cfEqual, cf, "CFEqual"},
		{&cfArrayGetCount, cf, "CFArrayGetCount"},
		{&cfArrayGetValueAtIndex, cf, "CFArrayGetValueAtIndex"},
		{&cfRelease, cf, "CFRelease"},
		{&memoryCopy, libSystem, "memcpy"},
		{&secCodeCopyGuestWithAttributes, sec, "SecCodeCopyGuestWithAttributes"},
		{&secRequirementCreateWithString, sec, "SecRequirementCreateWithString"},
		{&secCodeCheckValidityWithErrors, sec, "SecCodeCheckValidityWithErrors"},
		{&secCodeCopySigningInformation, sec, "SecCodeCopySigningInformation"},
	} {
		if _, err := purego.Dlsym(fn.lib, fn.name); err != nil {
			secErr = fmt.Errorf("trust: dlsym %s: %w", fn.name, err)
			return
		}
		purego.RegisterLibFunc(fn.target, fn.lib, fn.name)
	}

	// CF-object DATA symbols: dlsym returns the address of the value — deref.
	// The dictionary callbacks are STRUCTs: pass the dlsym address directly.
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
	infoEntsDictKey, derefErr = derefStringSym(sec, "kSecCodeInfoEntitlementsDict")
	if derefErr != nil {
		secErr = derefErr
		return
	}
	infoEntsKey, derefErr = derefStringSym(sec, "kSecCodeInfoEntitlements")
	if derefErr != nil {
		secErr = derefErr
		return
	}
	cfBooleanFalse, derefErr = derefStringSym(cf, "kCFBooleanFalse")
	if derefErr != nil {
		secErr = derefErr
		return
	}
	cfBooleanTrue, derefErr = derefStringSym(cf, "kCFBooleanTrue")
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
	var value uintptr
	memoryCopy(&value, sym, uintptr(strconv.IntSize/8))
	return value, nil
}

// Any failure is ErrUntrustedPeer; a missing token or verifier is
// ErrNoVerifier. Both fail closed.
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
	return requireHardenedRuntime(guest, req)
}

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

// CS_RUNTIME required; CS_GET_TASK_ALLOW and CS_DEBUGGED rejected; LV must hold via flags or an entitlements dict proven clean.
func requireHardenedRuntime(guest uintptr, req Requirement) error {
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
	if status&csGetTaskAllow != 0 {
		return fmt.Errorf("%w: peer permits debugger attachment (CS_GET_TASK_ALLOW, status 0x%x)", ErrUntrustedPeer, status)
	}
	if status&csDebugged != 0 {
		return fmt.Errorf("%w: peer ran under a debugger (CS_DEBUGGED, status 0x%x)", ErrUntrustedPeer, status)
	}
	if err := rejectInjectionEntitlements(info, status&(csRequireLV|csForcedLV) != 0); err != nil {
		return err
	}
	return requireEntitlements(info, req.entitlementRequirements())
}

func rejectInjectionEntitlements(info uintptr, lvProven bool) error {
	dict := cfDictionaryGetValue(info, infoEntsDictKey)
	if dict == 0 {
		if cfDictionaryGetValue(info, infoEntsKey) != 0 {
			return fmt.Errorf("%w: peer entitlements are not in dictionary form", ErrUntrustedPeer)
		}
		return nil
	}
	if cfGetTypeID(dict) != cfDictionaryGetTypeID() {
		return fmt.Errorf("%w: peer entitlements are not in dictionary form", ErrUntrustedPeer)
	}
	for _, ent := range injectionEntitlements {
		if lvProven && ent == entDisableLV {
			continue
		}
		key := cfStringCreateWithCString(0, ent+"\x00", kCFStringEncodingUTF8)
		if key == 0 {
			return fmt.Errorf("%w: CFStringCreateWithCString returned null", ErrNoVerifier)
		}
		val := cfDictionaryGetValue(dict, key)
		cfRelease(key)
		if val != 0 && (cfGetTypeID(val) != cfBooleanGetTypeID() || !cfEqual(val, cfBooleanFalse)) {
			return fmt.Errorf("%w: peer is signed with %s", ErrUntrustedPeer, ent)
		}
	}
	return nil
}

func requireEntitlements(info uintptr, requirements map[string]EntitlementRequirement) error {
	dict := cfDictionaryGetValue(info, infoEntsDictKey)
	if dict != 0 && cfGetTypeID(dict) != cfDictionaryGetTypeID() {
		return fmt.Errorf("%w: peer entitlements are not in dictionary form", ErrUntrustedPeer)
	}
	keys := make([]string, 0, len(requirements))
	for key := range requirements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if dict == 0 {
			return fmt.Errorf("%w: peer lacks required entitlement %s", ErrUntrustedPeer, key)
		}
		keyCF := cfStringCreateWithCString(0, key+"\x00", kCFStringEncodingUTF8)
		if keyCF == 0 {
			return fmt.Errorf("%w: CFStringCreateWithCString returned null", ErrNoVerifier)
		}
		value := cfDictionaryGetValue(dict, keyCF)
		cfRelease(keyCF)
		if value == 0 {
			return fmt.Errorf("%w: peer lacks required entitlement %s", ErrUntrustedPeer, key)
		}
		matched, err := matchEntitlement(value, requirements[key])
		if err != nil {
			return err
		}
		if !matched {
			return fmt.Errorf("%w: peer entitlement %s does not satisfy the required value", ErrUntrustedPeer, key)
		}
	}
	return nil
}

func matchEntitlement(value uintptr, requirement EntitlementRequirement) (bool, error) {
	switch requirement.Match {
	case EntitlementBoolean:
		if cfGetTypeID(value) != cfBooleanGetTypeID() {
			return false, nil
		}
		wanted := cfBooleanFalse
		if requirement.Boolean {
			wanted = cfBooleanTrue
		}
		return cfEqual(value, wanted), nil
	case EntitlementString:
		return matchCFString(value, requirement.String)
	case EntitlementStringArrayContains:
		if cfGetTypeID(value) != cfArrayGetTypeID() {
			return false, nil
		}
		for index := 0; index < cfArrayGetCount(value); index++ {
			matched, err := matchCFString(cfArrayGetValueAtIndex(value, index), requirement.String)
			if err != nil {
				return false, err
			}
			if matched {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("%w: unknown entitlement match %d", ErrNoVerifier, requirement.Match)
	}
}

func matchCFString(value uintptr, expected string) (bool, error) {
	if value == 0 || cfGetTypeID(value) != cfStringGetTypeID() {
		return false, nil
	}
	expectedCF := cfStringCreateWithCString(0, expected+"\x00", kCFStringEncodingUTF8)
	if expectedCF == 0 {
		return false, fmt.Errorf("%w: CFStringCreateWithCString returned null", ErrNoVerifier)
	}
	defer cfRelease(expectedCF)
	return cfEqual(value, expectedCF), nil
}
