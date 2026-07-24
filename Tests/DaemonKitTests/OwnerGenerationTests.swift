@testable import DaemonKit
import Foundation
import Testing

extension SocketTransportTests {
    struct OwnerGenerationTests {
        @Test func exactTextAndJSONRoundTrip() throws {
            let text = "0123456789abcdef0123456789abcdef"
            let generation = try OwnerGeneration(text)
            #expect(generation.value == text)
            #expect(generation.description == text)
            let encoded = try JSONEncoder().encode(generation)
            #expect(String(data: encoded, encoding: .utf8) == "\"\(text)\"")
            #expect(try JSONDecoder().decode(OwnerGeneration.self, from: encoded) == generation)
        }

        @Test(arguments: [
            "", "1", "0123456789abcdef0123456789abcde",
            "0123456789ABCDEF0123456789ABCDEF",
            "0123456789abcdef0123456789abcdeg",
            "00000000000000000000000000000000",
            "0123456789abcdef0123456789abcdef0",
        ])
        func rejectsNoncanonicalValues(_ value: String) {
            #expect(throws: OwnerGenerationValidationError.self) {
                try OwnerGeneration(value)
            }
        }
    }
}
