// wiregen regenerates daemonkit's session frame codec from schema.json: the
// Go package in internal/wire and the marked SessionFrameCodec codec section
// of Sources/DaemonKit/SessionTransport.swift.
package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strings"
)

//go:embed schema.json
var schemaJSON []byte

type schema struct {
	Magic           string        `json:"magic"`
	Version         uint16        `json:"version"`
	DefaultMaxFrame int           `json:"defaultMaxFrameBytes"`
	PrefixBytes     int           `json:"lengthPrefixBytes"`
	Header          []headerField `json:"header"`
	Kinds           []enumMember  `json:"kinds"`
	Flags           []enumMember  `json:"flags"`

	headerBytes int
}

type headerField struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Size       int    `json:"size"`
	Of         string `json:"of"`
	GoField    string `json:"goField"`
	SwiftField string `json:"swiftField"`

	offset int
}

type enumMember struct {
	Go       string `json:"go"`
	Swift    string `json:"swift"`
	Value    uint8  `json:"value"`
	GoDoc    string `json:"goDoc"`
	SwiftDoc string `json:"swiftDoc"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var s schema
	if err := json.Unmarshal(schemaJSON, &s); err != nil {
		return fmt.Errorf("wiregen: parse schema: %w", err)
	}
	if err := s.validate(); err != nil {
		return err
	}
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	source, err := goSource(&s)
	if err != nil {
		return err
	}
	goPath := filepath.Join(root, "internal", "wire", "frame.go")
	if err := os.WriteFile(goPath, source, 0o600); err != nil {
		return fmt.Errorf("wiregen: write %s: %w", goPath, err)
	}
	swiftPath := filepath.Join(root, "Sources", "DaemonKit", "SessionTransport.swift")
	if err := spliceSwift(swiftPath, swiftSection(&s)); err != nil {
		return err
	}
	return nil
}

func (s *schema) validate() error {
	if len(s.Magic) != 4 {
		return fmt.Errorf("wiregen: magic %q is not 4 bytes", s.Magic)
	}
	if s.Version == 0 {
		return fmt.Errorf("wiregen: version 0 is reserved")
	}
	if s.DefaultMaxFrame <= 0 {
		return fmt.Errorf("wiregen: defaultMaxFrameBytes %d", s.DefaultMaxFrame)
	}
	if s.PrefixBytes != 4 {
		return fmt.Errorf("wiregen: length prefix is a big-endian uint32, not %d bytes", s.PrefixBytes)
	}
	roles := map[string]int{}
	offset := 0
	for i := range s.Header {
		f := &s.Header[i]
		f.offset = offset
		offset += f.Size
		role := f.Kind
		switch f.Kind {
		case "magic":
			if f.Size != len(s.Magic) {
				return fmt.Errorf("wiregen: %s size %d does not carry magic %q", f.Name, f.Size, s.Magic)
			}
		case "version", "length":
			if f.Size != 2 {
				return fmt.Errorf("wiregen: %s size %d, want 2", f.Name, f.Size)
			}
			if f.Kind == "length" {
				if f.Of != "op" && f.Of != "tenant" {
					return fmt.Errorf("wiregen: %s counts unknown trailer field %q", f.Name, f.Of)
				}
				role += ":" + f.Of
			}
		case "frameKind", "frameFlags":
			if f.Size != 1 {
				return fmt.Errorf("wiregen: %s size %d, want 1", f.Name, f.Size)
			}
		case "uint":
			if f.Size != 4 && f.Size != 8 {
				return fmt.Errorf("wiregen: %s size %d, want 4 or 8", f.Name, f.Size)
			}
			if f.GoField == "" || f.SwiftField == "" {
				return fmt.Errorf("wiregen: %s names no struct field", f.Name)
			}
			role += ":" + f.GoField
		case "deadline":
			if f.Size != 8 {
				return fmt.Errorf("wiregen: %s size %d, want 8", f.Name, f.Size)
			}
			if f.GoField == "" || f.SwiftField == "" {
				return fmt.Errorf("wiregen: %s names no struct field", f.Name)
			}
		default:
			return fmt.Errorf("wiregen: %s has unknown kind %q", f.Name, f.Kind)
		}
		roles[role]++
	}
	s.headerBytes = offset
	for _, role := range []string{"magic", "version", "frameKind", "frameFlags", "deadline", "length:op", "length:tenant"} {
		if roles[role] != 1 {
			return fmt.Errorf("wiregen: header carries %d %s fields, want exactly 1", roles[role], role)
		}
	}
	for i, k := range s.Kinds {
		if int(k.Value) != i+1 {
			return fmt.Errorf("wiregen: kind %s = %d breaks the contiguous 1..n range valid() encodes", k.Go, k.Value)
		}
	}
	for _, f := range s.Flags {
		if f.Value == 0 || f.Value&(f.Value-1) != 0 {
			return fmt.Errorf("wiregen: flag %s = %d is not a single bit", f.Go, f.Value)
		}
	}
	return nil
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("wiregen: getwd: %w", err)
	}
	for probe := dir; ; probe = filepath.Dir(probe) {
		if _, err := os.Stat(filepath.Join(probe, "go.mod")); err == nil {
			return probe, nil
		}
		if filepath.Dir(probe) == probe {
			return "", fmt.Errorf("wiregen: no go.mod above %s", dir)
		}
	}
}

func (s *schema) field(kind, of string) headerField {
	for _, f := range s.Header {
		if f.Kind == kind && f.Of == of {
			return f
		}
	}
	panic("wiregen: validated header lost its " + kind + " field")
}

func (s *schema) goFlagMask() string {
	names := make([]string, len(s.Flags))
	for i, f := range s.Flags {
		names[i] = f.Go
	}
	if len(names) == 1 {
		return names[0]
	}
	return "(" + strings.Join(names, " | ") + ")"
}

func (s *schema) swiftFlagMask() string {
	if len(s.Flags) == 1 {
		return "." + s.Flags[0].Swift
	}
	names := make([]string, len(s.Flags))
	for i, f := range s.Flags {
		names[i] = "." + f.Swift
	}
	return "[" + strings.Join(names, ", ") + "]"
}

func (s *schema) maxFrameDoc() string {
	if s.DefaultMaxFrame%(1<<20) == 0 {
		return fmt.Sprintf("%d MiB", s.DefaultMaxFrame>>20)
	}
	return fmt.Sprintf("%d bytes", s.DefaultMaxFrame)
}

func goRange(f headerField) string {
	if f.offset == 0 {
		return fmt.Sprintf("body[:%d]", f.Size)
	}
	return fmt.Sprintf("body[%d:%d]", f.offset, f.offset+f.Size)
}

func goSource(s *schema) ([]byte, error) {
	var b strings.Builder
	p := func(format string, args ...any) {
		fmt.Fprintf(&b, format+"\n", args...)
	}
	p("// Code generated by wiregen from internal/wiregen/schema.json. DO NOT EDIT.")
	p("")
	p("package wire")
	p("")
	p("import (")
	p(`	"encoding/binary"`)
	p(`	"errors"`)
	p(`	"fmt"`)
	p(`	"math"`)
	p(")")
	p("")
	p("const (")
	p("	// ProtocolVersion is the exact transport version accepted by every peer.")
	p("	ProtocolVersion uint16 = %d", s.Version)
	p("	// DefaultMaxFrame caps one length-prefixed frame at %s.", s.maxFrameDoc())
	p("	DefaultMaxFrame = %d", s.DefaultMaxFrame)
	p("	frameHeaderSize = %d", s.headerBytes)
	p("	framePrefixSize = %d", s.PrefixBytes)
	p(")")
	p("")
	magic := make([]string, len(s.Magic))
	for i := range len(s.Magic) {
		magic[i] = fmt.Sprintf("%q", s.Magic[i])
	}
	p("var frameMagic = [%d]byte{%s}", len(s.Magic), strings.Join(magic, ", "))
	p("")
	p("var (")
	p("	// ErrFrameTooLarge means a declared frame exceeds DefaultMaxFrame.")
	p(`	ErrFrameTooLarge = errors.New("wire: frame exceeds maximum")`)
	p("	// ErrFrameTruncated means a packet ends in the middle of a frame.")
	p(`	ErrFrameTruncated = errors.New("wire: truncated frame")`)
	p("	// ErrProtocolVersion means a frame carries a protocol other than ProtocolVersion.")
	p(`	ErrProtocolVersion = errors.New("wire: unsupported protocol version")`)
	p("	// ErrInvalidFrame means a frame violates the structural contract.")
	p(`	ErrInvalidFrame = errors.New("wire: invalid frame")`)
	p(")")
	p("")
	p("// FrameKind identifies one session message.")
	p("type FrameKind uint8")
	p("")
	p("const (")
	for _, k := range s.Kinds {
		p("	// %s %s.", k.Go, k.GoDoc)
		p("	%s FrameKind = %d", k.Go, k.Value)
	}
	p(")")
	p("")
	p("// FrameFlags modifies a frame without changing its kind.")
	p("type FrameFlags uint8")
	p("")
	p("const (")
	for _, f := range s.Flags {
		p("	// %s %s.", f.Go, f.GoDoc)
		p("	%s FrameFlags = %d", f.Go, f.Value)
	}
	p(")")
	p("")
	p("// Op names a control-plane operation. Consumers define their own op values.")
	p("type Op string")
	p("")
	p("// Frame is the transport's fixed-header message. Payload stays opaque to wire.")
	p("type Frame struct {")
	for _, f := range s.Header {
		switch f.Kind {
		case "frameKind":
			p("	Kind FrameKind")
		case "frameFlags":
			p("	Flags FrameFlags")
		case "uint":
			p("	%s uint%d", f.GoField, f.Size*8)
		case "deadline":
			p("	%s int64", f.GoField)
		}
	}
	p("	Op Op")
	p("	Tenant string")
	p("	Payload []byte")
	p("}")
	p("")
	p("func (k FrameKind) valid() bool { return k >= %s && k <= %s }", s.Kinds[0].Go, s.Kinds[len(s.Kinds)-1].Go)
	p("")
	p("// EncodeFrame encodes one structurally valid frame body without its length prefix.")
	p("func EncodeFrame(frame Frame) ([]byte, error) {")
	p("	if !frame.Kind.valid() {")
	p(`	return nil, fmt.Errorf("%%w: kind %%d", ErrInvalidFrame, frame.Kind)`)
	p("	}")
	p("	if frame.Flags&^%s != 0 {", s.goFlagMask())
	p(`	return nil, fmt.Errorf("%%w: flags %%d", ErrInvalidFrame, frame.Flags)`)
	p("	}")
	p("	if len(frame.Op) > math.MaxUint16 {")
	p(`	return nil, fmt.Errorf("%%w: operation length %%d", ErrInvalidFrame, len(frame.Op))`)
	p("	}")
	p("	if len(frame.Tenant) > math.MaxUint16 {")
	p(`	return nil, fmt.Errorf("%%w: tenant length %%d", ErrInvalidFrame, len(frame.Tenant))`)
	p("	}")
	deadline := s.field("deadline", "")
	p("	if frame.%s < 0 {", deadline.GoField)
	p(`	return nil, fmt.Errorf("%%w: negative deadline %%d", ErrInvalidFrame, frame.%s)`, deadline.GoField)
	p("	}")
	p("	body := make([]byte, frameHeaderSize+len(frame.Op)+len(frame.Tenant)+len(frame.Payload))")
	for _, f := range s.Header {
		switch f.Kind {
		case "magic":
			p("	copy(%s, frameMagic[:])", goRange(f))
		case "version":
			p("	binary.BigEndian.PutUint16(%s, ProtocolVersion)", goRange(f))
		case "frameKind":
			p("	body[%d] = byte(frame.Kind)", f.offset)
		case "frameFlags":
			p("	body[%d] = byte(frame.Flags)", f.offset)
		case "uint":
			p("	binary.BigEndian.PutUint%d(%s, frame.%s)", f.Size*8, goRange(f), f.GoField)
		case "deadline":
			p("	binary.BigEndian.PutUint64(%s, uint64(frame.%s))", goRange(f), f.GoField)
		case "length":
			p("	binary.BigEndian.PutUint16(%s, uint16(len(frame.%s)))", goRange(f), goTrailerField(f.Of))
		}
	}
	p("	off := frameHeaderSize")
	p("	off += copy(body[off:], frame.Op)")
	p("	off += copy(body[off:], frame.Tenant)")
	p("	copy(body[off:], frame.Payload)")
	p("	return body, nil")
	p("}")
	p("")
	p("// DecodeFrame decodes one version-exact frame body without its length prefix.")
	p("func DecodeFrame(body []byte) (Frame, error) {")
	p("	if len(body) < frameHeaderSize {")
	p(`	return Frame{}, fmt.Errorf("%%w: body length %%d", ErrInvalidFrame, len(body))`)
	p("	}")
	p("	if string(%s) != string(frameMagic[:]) {", goRange(s.field("magic", "")))
	p(`	return Frame{}, fmt.Errorf("%%w: magic", ErrInvalidFrame)`)
	p("	}")
	p("	version := binary.BigEndian.Uint16(%s)", goRange(s.field("version", "")))
	p("	if version != ProtocolVersion {")
	p(`	return Frame{}, fmt.Errorf("%%w: got %%d, want %%d", ErrProtocolVersion, version, ProtocolVersion)`)
	p("	}")
	p("	kind := FrameKind(body[%d])", s.field("frameKind", "").offset)
	p("	if !kind.valid() {")
	p(`	return Frame{}, fmt.Errorf("%%w: kind %%d", ErrInvalidFrame, kind)`)
	p("	}")
	p("	flags := FrameFlags(body[%d])", s.field("frameFlags", "").offset)
	p("	if flags&^%s != 0 {", s.goFlagMask())
	p(`	return Frame{}, fmt.Errorf("%%w: flags %%d", ErrInvalidFrame, flags)`)
	p("	}")
	p("	deadline := binary.BigEndian.Uint64(%s)", goRange(deadline))
	p("	if deadline > math.MaxInt64 {")
	p(`	return Frame{}, fmt.Errorf("%%w: deadline %%d exceeds int64", ErrInvalidFrame, deadline)`)
	p("	}")
	p("	opLength := int(binary.BigEndian.Uint16(%s))", goRange(s.field("length", "op")))
	p("	tenantLength := int(binary.BigEndian.Uint16(%s))", goRange(s.field("length", "tenant")))
	p("	if frameHeaderSize+opLength+tenantLength > len(body) {")
	p(`	return Frame{}, fmt.Errorf("%%w: routing lengths", ErrInvalidFrame)`)
	p("	}")
	p("	off := frameHeaderSize")
	p("	op := Op(body[off : off+opLength])")
	p("	off += opLength")
	p("	tenant := string(body[off : off+tenantLength])")
	p("	off += tenantLength")
	p("	return Frame{")
	for _, f := range s.Header {
		switch f.Kind {
		case "frameKind":
			p("	Kind: kind,")
		case "frameFlags":
			p("	Flags: flags,")
		case "uint":
			p("	%s: binary.BigEndian.Uint%d(%s),", f.GoField, f.Size*8, goRange(f))
		case "deadline":
			p("	%s: int64(deadline),", f.GoField)
		}
	}
	p("	Op: op,")
	p("	Tenant: tenant,")
	p("	Payload: append([]byte(nil), body[off:]...),")
	p("	}, nil")
	p("}")
	p("")
	p("// EncodePacket encodes one frame as a length-prefixed packet under DefaultMaxFrame.")
	p("func EncodePacket(frame Frame) ([]byte, error) {")
	p("	body, err := EncodeFrame(frame)")
	p("	if err != nil {")
	p("	return nil, err")
	p("	}")
	p("	if len(body) > DefaultMaxFrame {")
	p(`	return nil, fmt.Errorf("%%w: %%d > %%d", ErrFrameTooLarge, len(body), DefaultMaxFrame)`)
	p("	}")
	p("	packet := make([]byte, framePrefixSize+len(body))")
	p("	binary.BigEndian.PutUint32(packet[:framePrefixSize], uint32(len(body)))")
	p("	copy(packet[framePrefixSize:], body)")
	p("	return packet, nil")
	p("}")
	p("")
	p("// DecodePacket decodes one exact length-prefixed packet under DefaultMaxFrame.")
	p("func DecodePacket(packet []byte) (Frame, error) {")
	p("	if len(packet) < framePrefixSize {")
	p("	return Frame{}, ErrFrameTruncated")
	p("	}")
	p("	declared := int(binary.BigEndian.Uint32(packet[:framePrefixSize]))")
	p("	if declared > DefaultMaxFrame {")
	p(`	return Frame{}, fmt.Errorf("%%w: %%d > %%d", ErrFrameTooLarge, declared, DefaultMaxFrame)`)
	p("	}")
	p("	body := packet[framePrefixSize:]")
	p("	if len(body) < declared {")
	p("	return Frame{}, ErrFrameTruncated")
	p("	}")
	p("	if len(body) > declared {")
	p(`	return Frame{}, fmt.Errorf("%%w: %%d trailing bytes", ErrInvalidFrame, len(body)-declared)`)
	p("	}")
	p("	return DecodeFrame(body)")
	p("}")
	source, err := format.Source([]byte(b.String()))
	if err != nil {
		return nil, fmt.Errorf("wiregen: format generated Go: %w", err)
	}
	return source, nil
}

func goTrailerField(of string) string {
	switch of {
	case "op":
		return "Op"
	case "tenant":
		return "Tenant"
	}
	panic("wiregen: validated trailer lost field " + of)
}

func swiftTrailerLocal(of string) string {
	switch of {
	case "op":
		return "operation"
	case "tenant":
		return "tenant"
	}
	panic("wiregen: validated trailer lost field " + of)
}

func swiftGroupedInt(value int) string {
	digits := fmt.Sprintf("%d", value)
	if len(digits) < 6 {
		return digits
	}
	var grouped []string
	for len(digits) > 3 {
		grouped = append([]string{digits[len(digits)-3:]}, grouped...)
		digits = digits[:len(digits)-3]
	}
	return strings.Join(append([]string{digits}, grouped...), "_")
}

func swiftSection(s *schema) string {
	var b strings.Builder
	p := func(format string, args ...any) {
		fmt.Fprintf(&b, format+"\n", args...)
	}
	deadline := s.field("deadline", "")
	p("// Code generated by wiregen from internal/wiregen/schema.json. DO NOT EDIT.")
	p("")
	p("/// Exact protocol version shared by daemonkit's Go and Swift session transports.")
	p("public let daemonKitSessionProtocolVersion: UInt16 = %d", s.Version)
	p("")
	p("/// Default maximum encoded frame body: %s.", s.maxFrameDoc())
	p("public let daemonKitDefaultMaximumFrameBytes = %s", swiftGroupedInt(s.DefaultMaxFrame))
	p("")
	p("/// A session frame kind.")
	p("public enum SessionFrameKind: UInt8, Sendable {")
	for _, k := range s.Kinds {
		p("    case %s = %d", k.Swift, k.Value)
	}
	p("}")
	p("")
	p("/// Flags carried by a session frame.")
	p("public struct SessionFrameFlags: OptionSet, Sendable {")
	p("    public let rawValue: UInt8")
	p("")
	p("    public init(rawValue: UInt8) {")
	p("        self.rawValue = rawValue")
	p("    }")
	for _, f := range s.Flags {
		p("")
		p("    /// %s", f.SwiftDoc)
		p("    public static let %s = SessionFrameFlags(rawValue: %d)", f.Swift, f.Value)
	}
	p("}")
	p("")
	p("extension SessionFrameCodec {")
	p("    static let headerBytes = %d", s.headerBytes)
	p("    static let magic = Data(%q.utf8)", s.Magic)
	p("")
	p("    static func encode(_ frame: SessionFrame) throws -> Data {")
	p("        try validate(frame)")
	p("        let operation = Data(frame.operation.utf8)")
	p("        let tenant = Data(frame.tenant.utf8)")
	p("        guard operation.count <= Int(UInt16.max), tenant.count <= Int(UInt16.max) else {")
	p("            throw SessionTransportError.invalidFrame(\"routing field too long\")")
	p("        }")
	p("        var body = Data()")
	for _, f := range s.Header {
		switch f.Kind {
		case "magic":
			p("        body.append(magic)")
		case "version":
			p("        body.appendUInt16(daemonKitSessionProtocolVersion)")
		case "frameKind":
			p("        body.append(frame.kind.rawValue)")
		case "frameFlags":
			p("        body.append(frame.flags.rawValue)")
		case "uint":
			p("        body.appendUInt%d(frame.%s)", f.Size*8, f.SwiftField)
		case "deadline":
			p("        body.appendUInt64(UInt64(bitPattern: frame.%s))", f.SwiftField)
		case "length":
			p("        body.appendUInt16(UInt16(%s.count))", swiftTrailerLocal(f.Of))
		}
	}
	p("        body.append(operation)")
	p("        body.append(tenant)")
	p("        body.append(frame.payload)")
	p("        return body")
	p("    }")
	p("")
	p("    static func decode(_ body: Data) throws -> SessionFrame {")
	p("        guard body.count >= headerBytes else {")
	p("            throw SessionTransportError.invalidFrame(\"short header\")")
	p("        }")
	p("        guard body.prefix(%d) == magic else {", len(s.Magic))
	p("            throw SessionTransportError.invalidFrame(\"magic\")")
	p("        }")
	p("        let version = body.uint16(at: %d)", s.field("version", "").offset)
	p("        guard version == daemonKitSessionProtocolVersion else {")
	p("            throw SessionTransportError.unsupportedProtocolVersion(version)")
	p("        }")
	p("        guard let kind = SessionFrameKind(rawValue: body[%d]) else {", s.field("frameKind", "").offset)
	p("            throw SessionTransportError.invalidFrame(\"kind\")")
	p("        }")
	p("        let flags = SessionFrameFlags(rawValue: body[%d])", s.field("frameFlags", "").offset)
	p("        guard flags.subtracting(%s).isEmpty else {", s.swiftFlagMask())
	p("            throw SessionTransportError.invalidFrame(\"flags\")")
	p("        }")
	p("        let operationLength = Int(body.uint16(at: %d))", s.field("length", "op").offset)
	p("        let tenantLength = Int(body.uint16(at: %d))", s.field("length", "tenant").offset)
	p("        let routingEnd = headerBytes + operationLength + tenantLength")
	p("        guard routingEnd <= body.count else {")
	p("            throw SessionTransportError.invalidFrame(\"routing lengths\")")
	p("        }")
	p("        let operationRange = headerBytes ..< headerBytes + operationLength")
	p("        let tenantRange = operationRange.upperBound ..< routingEnd")
	p("        guard let operation = String(data: body[operationRange], encoding: .utf8),")
	p("              let tenant = String(data: body[tenantRange], encoding: .utf8)")
	p("        else {")
	p("            throw SessionTransportError.invalidFrame(\"routing UTF-8\")")
	p("        }")
	p("        let frame = SessionFrame(")
	p("            kind: kind,")
	p("            flags: flags,")
	for _, f := range s.Header {
		if f.Kind != "uint" {
			continue
		}
		p("            %s: body.uint%d(at: %d),", f.SwiftField, f.Size*8, f.offset)
	}
	p("            %s: Int64(bitPattern: body.uint64(at: %d)),", deadline.SwiftField, deadline.offset)
	p("            operation: operation,")
	p("            tenant: tenant,")
	p("            payload: body.subdata(in: routingEnd ..< body.count)")
	p("        )")
	p("        try validate(frame)")
	p("        return frame")
	p("    }")
	p("}")
	p("")
	return b.String()
}

func spliceSwift(path, generated string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("wiregen: read %s: %w", path, err)
	}
	const begin = "// wiregen:begin\n"
	const end = "// wiregen:end"
	text := string(raw)
	beginAt := strings.Index(text, begin)
	endAt := strings.Index(text, end)
	if beginAt < 0 || endAt < beginAt {
		return fmt.Errorf("wiregen: %s carries no wiregen marker pair", path)
	}
	spliced := text[:beginAt+len(begin)] + generated + text[endAt:]
	if err := os.WriteFile(path, []byte(spliced), 0o600); err != nil {
		return fmt.Errorf("wiregen: write %s: %w", path, err)
	}
	return nil
}
