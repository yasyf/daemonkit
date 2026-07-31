@testable import DaemonKit
import Darwin
import Foundation
import Testing

@Suite(.timeLimit(.minutes(1)))
struct BrokerHandoffTests {
    @Test func makeRequestProducesDecodableNonceOnlyPayload() throws {
        let request = try BrokerHandoffCodec.makeRequest()
        #expect(request.nonce.count == 32)
        let decoded = try BrokerHandoffCodec.decode(request.payload)
        #expect(decoded == request.nonce)
    }

    @Test func encodedPayloadCarriesExactlyTheNonceField() throws {
        let nonce = Data((0 ..< 32).map { UInt8($0) })
        let payload = try BrokerHandoffCodec.encode(nonce: nonce)
        let object = try #require(
            JSONSerialization.jsonObject(with: payload) as? [String: Any]
        )
        #expect(Set(object.keys) == ["nonce"])
        #expect(object["nonce"] as? String == nonce.base64EncodedString())
    }

    @Test func decodeRejectsUnexpectedFields() throws {
        let payload = Data(#"{"nonce":"AAAA","runtime_identity":{}}"#.utf8)
        #expect(throws: BrokerHandoffError.invalidPayload) {
            _ = try BrokerHandoffCodec.decode(payload)
        }
    }

    @Test func decodeRejectsWrongNonceLength() throws {
        let short = Data((0 ..< 16).map { UInt8($0) })
        let payload = Data("{\"nonce\":\"\(short.base64EncodedString())\"}".utf8)
        #expect(throws: BrokerHandoffError.invalidPayload) {
            _ = try BrokerHandoffCodec.decode(payload)
        }
    }

    @Test func encodeRejectsWrongNonceLength() throws {
        #expect(throws: BrokerHandoffError.invalidPayload) {
            _ = try BrokerHandoffCodec.encode(nonce: Data(count: 8))
        }
    }
}
