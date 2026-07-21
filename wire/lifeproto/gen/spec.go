package main

// spec.go is the single source of truth for the lifeproto envelope: the
// protocol version, the op strings, and every message's flat field set. Any
// change here is an intentional protocol break: increment the exact version.

// fieldKind pairs a wire field's Go and Swift types. slice marks a []string,
// whose Go constructor nil-normalizes to [] so it never encodes as null.
type fieldKind struct {
	goType    string
	swiftType string
	slice     bool
}

var (
	stringKind = fieldKind{goType: "string", swiftType: "String"}
	intKind    = fieldKind{goType: "int", swiftType: "Int"}
	boolKind   = fieldKind{goType: "bool", swiftType: "Bool"}
)

// field is one op-specific wire field beyond the shared {v, op} header.
type field struct {
	json   string
	goName string
	name   string
	kind   fieldKind
	doc    string
}

type message struct {
	name    string
	op      string
	opConst string
	request bool
	fields  []field
	typeDoc string
	ctorDoc string
}

// op is a lifecycle operation: a Go const, its frozen wire value, and its Swift enum case.
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
	fBuild    = field{json: "build", goName: "Build", name: "build", kind: stringKind, doc: "The peer's build identifier."}
	fProtocol = field{json: "protocol", goName: "Protocol", name: "protocolVersion", kind: intKind, doc: "The exact transport and lifecycle protocol version."}
	fPID      = field{json: "pid", goName: "PID", name: "pid", kind: intKind, doc: "The peer's process id."}
	fState    = field{json: "state", goName: "State", name: "state", kind: stringKind, doc: "The peer's lifecycle state, e.g. \"healthy\" or \"degraded\"."}
	fDraining = field{json: "draining", goName: "Draining", name: "draining", kind: boolKind, doc: "Whether the peer is draining."}
	fBusy     = field{json: "busy", goName: "Busy", name: "busy", kind: boolKind, doc: "Whether the peer is serving work."}
	fOK       = field{json: "ok", goName: "OK", name: "ok", kind: boolKind, doc: "Whether the peer accepted the request."}
)

// lifeproto is the frozen lifecycle protocol schema.
var lifeproto = schema{
	version: 1,
	ops: []op{
		{constName: "OpHealth", value: "health"},
		{constName: "OpShutdown", value: "shutdown"},
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
			fields:  []field{fBuild, fProtocol, fPID, fState, fDraining, fBusy},
			typeDoc: "HealthResponse is the peer's exact-protocol health snapshot.",
			ctorDoc: "builds a health snapshot.",
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
