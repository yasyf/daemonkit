package trust

import (
	"encoding/asn1"
	"encoding/binary"
	"fmt"
	"sort"
	"unicode/utf8"
)

const (
	derEntitlementsMagic = 0xfade7172
	maxEntitlementsBlob  = 1 << 20
	maxEntitlementDepth  = 16
	maxEntitlementCount  = 4096
	maxEntitlementText   = 1 << 20
)

type entKind uint8

const (
	entOpaque entKind = iota
	entBool
	entInt
	entString
	entArray
	entDict
)

// entValue is one decoded entitlement value. entOpaque covers every shape AMFI
// does not emit, satisfies no required-entitlement predicate, and clears no
// injection rejection.
type entValue struct {
	kind    entKind
	boolean bool
	str     string
	array   []entValue
}

// entitlements is one decoded DER entitlements dictionary. A nil map is the
// measured shape of a Developer ID binary signed with no entitlements at all.
type entitlements map[string]entValue

type entBudget struct {
	elements int
	text     int
}

func (b *entBudget) element() error {
	b.elements++
	if b.elements > maxEntitlementCount {
		return fmt.Errorf("%w: entitlements carry more than %d elements", ErrNoVerifier, maxEntitlementCount)
	}
	return nil
}

func (b *entBudget) take(n int) error {
	b.text += n
	if b.text > maxEntitlementText {
		return fmt.Errorf("%w: entitlements carry more than %d bytes of text", ErrNoVerifier, maxEntitlementText)
	}
	return nil
}

// decodeEntitlementsBlob decodes one CS_OPS_DER_ENTITLEMENTS_BLOB payload.
// blob is the whole kernel buffer, 8-byte blob header included. An all-zero
// header is the measured shape of "this binary carries no entitlements" and
// decodes to a nil dictionary; every other non-magic header is a denial, so
// unexplained bytes can never read as "no entitlements".
func decodeEntitlementsBlob(blob []byte) (entitlements, error) {
	if len(blob) < 8 {
		return nil, fmt.Errorf("%w: entitlements blob is %d bytes, want at least 8", ErrNoVerifier, len(blob))
	}
	magic := binary.BigEndian.Uint32(blob[0:4])
	length := binary.BigEndian.Uint32(blob[4:8])
	if magic == 0 && length == 0 {
		return nil, nil
	}
	if magic != derEntitlementsMagic {
		return nil, fmt.Errorf("%w: entitlements blob magic 0x%08x is not 0x%08x", ErrNoVerifier, magic, derEntitlementsMagic)
	}
	if length < 8 || length > maxEntitlementsBlob || int(length) > len(blob) {
		return nil, fmt.Errorf("%w: entitlements blob length header %d is outside [8, min(%d, %d)]",
			ErrNoVerifier, length, maxEntitlementsBlob, len(blob))
	}
	return decodeEntitlements(blob[8:length])
}

// decodeEntitlements decodes AMFI's DER entitlements grammar, measured on
// macOS 26.5.2: [APPLICATION 16] { INTEGER version, [CONTEXT 16] { SEQUENCE {
// UTF8String key, value } ... } }.
func decodeEntitlements(der []byte) (entitlements, error) {
	var root asn1.RawValue
	rest, err := asn1.Unmarshal(der, &root)
	if err != nil {
		return nil, fmt.Errorf("%w: entitlements DER: %w", ErrNoVerifier, err)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: entitlements DER carries %d trailing bytes", ErrNoVerifier, len(rest))
	}
	if root.Class != asn1.ClassApplication || root.Tag != 16 || !root.IsCompound {
		return nil, fmt.Errorf("%w: entitlements root is class %d tag %d, want [APPLICATION 16]", ErrNoVerifier, root.Class, root.Tag)
	}
	var version int
	body, err := asn1.Unmarshal(root.Bytes, &version)
	if err != nil {
		return nil, fmt.Errorf("%w: entitlements version: %w", ErrNoVerifier, err)
	}
	if version != 1 {
		return nil, fmt.Errorf("%w: entitlements version %d is not 1", ErrNoVerifier, version)
	}
	var dictionary asn1.RawValue
	body, err = asn1.Unmarshal(body, &dictionary)
	if err != nil {
		return nil, fmt.Errorf("%w: entitlements dictionary: %w", ErrNoVerifier, err)
	}
	if len(body) != 0 {
		return nil, fmt.Errorf("%w: entitlements body carries %d trailing bytes", ErrNoVerifier, len(body))
	}
	if !isEntitlementDictionary(dictionary) {
		return nil, fmt.Errorf("%w: entitlements dictionary is class %d tag %d, want [CONTEXT 16]",
			ErrNoVerifier, dictionary.Class, dictionary.Tag)
	}
	return decodeEntitlementDictionary(dictionary.Bytes, 1, &entBudget{})
}

func isEntitlementDictionary(value asn1.RawValue) bool {
	if !value.IsCompound {
		return false
	}
	if value.Class == asn1.ClassContextSpecific && value.Tag == 16 {
		return true
	}
	return value.Class == asn1.ClassUniversal && value.Tag == asn1.TagSet
}

func decodeEntitlementDictionary(body []byte, depth int, budget *entBudget) (entitlements, error) {
	if depth > maxEntitlementDepth {
		return nil, fmt.Errorf("%w: entitlements nest deeper than %d", ErrNoVerifier, maxEntitlementDepth)
	}
	decoded := make(entitlements)
	for len(body) > 0 {
		var element asn1.RawValue
		rest, err := asn1.Unmarshal(body, &element)
		if err != nil {
			return nil, fmt.Errorf("%w: entitlement element: %w", ErrNoVerifier, err)
		}
		body = rest
		if element.Class != asn1.ClassUniversal || element.Tag != asn1.TagSequence || !element.IsCompound {
			return nil, fmt.Errorf("%w: entitlement element is class %d tag %d, want SEQUENCE", ErrNoVerifier, element.Class, element.Tag)
		}
		if err := budget.element(); err != nil {
			return nil, err
		}
		var key asn1.RawValue
		valueBytes, err := asn1.Unmarshal(element.Bytes, &key)
		if err != nil {
			return nil, fmt.Errorf("%w: entitlement key: %w", ErrNoVerifier, err)
		}
		name, err := decodeEntitlementText(key, budget)
		if err != nil {
			return nil, err
		}
		if _, duplicate := decoded[name]; duplicate {
			return nil, fmt.Errorf("%w: entitlements carry duplicate key %q", ErrNoVerifier, name)
		}
		value, rest, err := decodeEntitlementValue(valueBytes, depth+1, budget)
		if err != nil {
			return nil, err
		}
		if len(rest) != 0 {
			return nil, fmt.Errorf("%w: entitlement %q carries %d trailing bytes", ErrNoVerifier, name, len(rest))
		}
		decoded[name] = value
	}
	return decoded, nil
}

func decodeEntitlementText(value asn1.RawValue, budget *entBudget) (string, error) {
	if value.Class != asn1.ClassUniversal || value.Tag != asn1.TagUTF8String || value.IsCompound {
		return "", fmt.Errorf("%w: entitlement string is class %d tag %d, want UTF8String", ErrNoVerifier, value.Class, value.Tag)
	}
	if err := budget.take(len(value.Bytes)); err != nil {
		return "", err
	}
	text := string(value.Bytes)
	if !utf8.ValidString(text) {
		return "", fmt.Errorf("%w: entitlement string is not valid UTF-8", ErrNoVerifier)
	}
	return text, nil
}

func decodeEntitlementValue(body []byte, depth int, budget *entBudget) (entValue, []byte, error) {
	if depth > maxEntitlementDepth {
		return entValue{}, nil, fmt.Errorf("%w: entitlements nest deeper than %d", ErrNoVerifier, maxEntitlementDepth)
	}
	var raw asn1.RawValue
	rest, err := asn1.Unmarshal(body, &raw)
	if err != nil {
		return entValue{}, nil, fmt.Errorf("%w: entitlement value: %w", ErrNoVerifier, err)
	}
	if isEntitlementDictionary(raw) {
		if _, err := decodeEntitlementDictionary(raw.Bytes, depth+1, budget); err != nil {
			return entValue{}, nil, err
		}
		return entValue{kind: entDict}, rest, nil
	}
	if raw.Class != asn1.ClassUniversal {
		return entValue{kind: entOpaque}, rest, nil
	}
	switch {
	case raw.Tag == asn1.TagBoolean && !raw.IsCompound:
		if len(raw.Bytes) != 1 || (raw.Bytes[0] != 0x00 && raw.Bytes[0] != 0xff) {
			return entValue{}, nil, fmt.Errorf("%w: entitlement boolean is not a DER 0x00 or 0xff", ErrNoVerifier)
		}
		return entValue{kind: entBool, boolean: raw.Bytes[0] == 0xff}, rest, nil
	case raw.Tag == asn1.TagInteger && !raw.IsCompound:
		return entValue{kind: entInt}, rest, nil
	case raw.Tag == asn1.TagUTF8String && !raw.IsCompound:
		text, err := decodeEntitlementText(raw, budget)
		if err != nil {
			return entValue{}, nil, err
		}
		return entValue{kind: entString, str: text}, rest, nil
	case raw.Tag == asn1.TagSequence && raw.IsCompound:
		array, err := decodeEntitlementArray(raw.Bytes, depth+1, budget)
		if err != nil {
			return entValue{}, nil, err
		}
		return entValue{kind: entArray, array: array}, rest, nil
	default:
		return entValue{kind: entOpaque}, rest, nil
	}
}

func decodeEntitlementArray(body []byte, depth int, budget *entBudget) ([]entValue, error) {
	var array []entValue
	for len(body) > 0 {
		if err := budget.element(); err != nil {
			return nil, err
		}
		value, rest, err := decodeEntitlementValue(body, depth, budget)
		if err != nil {
			return nil, err
		}
		array = append(array, value)
		body = rest
	}
	return array, nil
}

// rejectInjection denies a peer signed with any injection entitlement. A key
// that is absent, or present with the boolean false, passes; a key present
// with any other value of any shape denies.
func rejectInjection(ents entitlements, req Requirement) error {
	for _, name := range injectionEntitlements {
		if req.AllowJIT && name == entAllowJIT {
			continue
		}
		value, present := ents[name]
		if !present {
			continue
		}
		if value.kind == entBool && !value.boolean {
			continue
		}
		return fmt.Errorf("%w: peer is signed with %s", ErrUntrustedPeer, name)
	}
	return nil
}

// requireEntitlements denies a peer that does not satisfy every required
// entitlement predicate. Keys are checked in sorted order, so the first denial
// is deterministic.
func requireEntitlements(ents entitlements, req Requirement) error {
	requirements := req.entitlementRequirements()
	keys := make([]string, 0, len(requirements))
	for key := range requirements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, present := ents[key]
		if !present {
			return fmt.Errorf("%w: peer lacks required entitlement %s", ErrUntrustedPeer, key)
		}
		if !matchEntitlement(value, requirements[key]) {
			return fmt.Errorf("%w: peer entitlement %s does not satisfy the required value", ErrUntrustedPeer, key)
		}
	}
	return nil
}

func matchEntitlement(value entValue, requirement EntitlementRequirement) bool {
	switch requirement.Match {
	case EntitlementBoolean:
		return value.kind == entBool && value.boolean == requirement.Boolean
	case EntitlementString:
		return value.kind == entString && value.str == requirement.String
	case EntitlementStringArrayContains:
		if value.kind != entArray {
			return false
		}
		for _, element := range value.array {
			if element.kind == entString && element.str == requirement.String {
				return true
			}
		}
		return false
	default:
		return false
	}
}
