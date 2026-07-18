package main

// spec.go is the single source of truth for the lifeproto lifecycle envelope:
// the protocol version, the op strings, and every message's flat field set.
// The generator (main.go) reads this table and emits both the Go binding
// (wire/lifeproto/lifeproto.go) and the Swift binding
// (Sources/DaemonKit/LifecycleWire.swift) so the two languages can never drift.
// The wire shape is FLAT: {"v":1,"op":<op>, ...fields}. It is frozen — field
// names, op strings, and field order are a compatibility contract with deployed
// peers, pinned by the shared golden fixture. Changes here are additive-only.

// fieldKind pairs a wire field's Go and Swift types. slice marks a []string,
// whose Go constructor nil-normalizes to [] so it never encodes as null.
type fieldKind struct {
	goType    string
	swiftType string
	slice     bool
}

var (
	stringKind  = fieldKind{goType: "string", swiftType: "String"}
	intKind     = fieldKind{goType: "int", swiftType: "Int"}
	boolKind    = fieldKind{goType: "bool", swiftType: "Bool"}
	stringsKind = fieldKind{goType: "[]string", swiftType: "[String]", slice: true}
)

// field is one op-specific wire field beyond the shared {v, op} header. json is
// the frozen wire key, goName the Go struct field, name the Swift property and
// the constructor parameter shared by both languages.
type field struct {
	json   string
	goName string
	name   string
	kind   fieldKind
	doc    string
}

// message is one request or response type. fields are the op-specific fields in
// wire order; the {v, op} header is prepended by the generator.
type message struct {
	name    string
	op      string
	opConst string
	request bool
	fields  []field
	typeDoc string
	ctorDoc string
}

// op is a lifecycle operation: a Go const, its frozen wire value, and its Swift
// enum case.
type op struct {
	constName string
	value     string
}

// schema is the whole declarative table the generator consumes.
type schema struct {
	version  int
	ops      []op
	messages []message
}

var (
	fVersion  = field{json: "version", goName: "Version", name: "version", kind: stringKind, doc: "The peer's own build version. Never compared for capability — Features is the only source of capability truth."}
	fPID      = field{json: "pid", goName: "PID", name: "pid", kind: intKind, doc: "The peer's process id."}
	fState    = field{json: "state", goName: "State", name: "state", kind: stringKind, doc: "The peer's lifecycle state, e.g. \"healthy\" or \"degraded\"."}
	fDraining = field{json: "draining", goName: "Draining", name: "draining", kind: boolKind, doc: "Whether the peer is draining."}
	fBusy     = field{json: "busy", goName: "Busy", name: "busy", kind: boolKind, doc: "Whether the peer is serving work."}
	fFeatures = field{json: "features", goName: "Features", name: "features", kind: stringsKind, doc: "The peer's advertised feature bits; the sole source of capability truth."}
	fOK       = field{json: "ok", goName: "OK", name: "ok", kind: boolKind, doc: "Whether the peer accepted the request."}
)

// lifeproto is the frozen lifecycle protocol schema.
var lifeproto = schema{
	version: 1,
	ops: []op{
		{constName: "OpHealth", value: "health"},
		{constName: "OpShutdown", value: "shutdown"},
		{constName: "OpHello", value: "hello"},
		{constName: "OpHandoff", value: "handoff"},
	},
	messages: []message{
		{
			name: "HealthRequest", op: "health", opConst: "OpHealth", request: true,
			typeDoc: "HealthRequest asks the peer for its health snapshot.",
			ctorDoc: "builds a health request at the current protocol version.",
		},
		{
			name: "HealthResponse", op: "health", opConst: "OpHealth",
			fields:  []field{fVersion, fPID, fState, fDraining, fBusy, fFeatures},
			typeDoc: "HealthResponse is the peer's health snapshot. Features is the only source of capability truth — never a version compare.",
			ctorDoc: "builds a health snapshot; a nil features slice encodes as [].",
		},
		{
			name: "ShutdownRequest", op: "shutdown", opConst: "OpShutdown", request: true,
			typeDoc: "ShutdownRequest asks the peer to shut down.",
			ctorDoc: "builds a shutdown request.",
		},
		{
			name: "ShutdownResponse", op: "shutdown", opConst: "OpShutdown",
			fields:  []field{fOK},
			typeDoc: "ShutdownResponse acknowledges a shutdown request.",
			ctorDoc: "builds a shutdown acknowledgement.",
		},
		{
			name: "HelloRequest", op: "hello", opConst: "OpHello", request: true,
			typeDoc: "HelloRequest opens the capability handshake.",
			ctorDoc: "builds a hello request.",
		},
		{
			name: "HelloResponse", op: "hello", opConst: "OpHello",
			fields:  []field{fFeatures},
			typeDoc: "HelloResponse announces the peer's advertised feature bits.",
			ctorDoc: "builds a hello announcement; a nil features slice encodes as [].",
		},
		{
			name: "HandoffRequest", op: "handoff", opConst: "OpHandoff", request: true,
			typeDoc: "HandoffRequest asks the peer to release its socket for a successor.",
			ctorDoc: "builds a handoff request.",
		},
		{
			name: "HandoffResponse", op: "handoff", opConst: "OpHandoff",
			fields:  []field{fOK},
			typeDoc: "HandoffResponse acknowledges a handoff request.",
			ctorDoc: "builds a handoff acknowledgement.",
		},
	},
}
