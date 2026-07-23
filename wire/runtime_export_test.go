package wire

// ConfigureProtectedForTest sets protected capacity for external wire tests.
func ConfigureProtectedForTest(server *Server, classifier ProtectedSessionClassifier, reserved int) {
	server.protectedSessionClassifier = classifier
	server.reservedProtectedSessions = reserved
}
