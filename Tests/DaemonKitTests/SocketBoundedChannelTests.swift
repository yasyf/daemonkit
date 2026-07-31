@testable import DaemonKit
import Foundation
import Testing

@Suite(.serialized, .timeLimit(.minutes(1)))
struct SocketBoundedChannelTests {
    @Test func sequenceRejectsBeforeWrappingToZero() throws {
        var sequence = SessionSequence(next: UInt32.max - 1)
        #expect(try sequence.take() == UInt32.max - 1)
        #expect(try sequence.take() == UInt32.max)
        #expect(throws: SessionTransportError.invalidFrame("stream sequence exhausted")) {
            try sequence.take()
        }
    }

    @Test func canceledResponseProducerDoesNotStrandWhenFull() async {
        let channel = SocketBoundedChannel<SocketRequestChunk>(capacity: 1)
        #expect(await channel.send(chunk(0)))
        await expectCanceledFullSendSettles(channel, value: chunk(1))
    }

    @Test func canceledEventProducerDoesNotStrandWhenFull() async {
        let channel = SocketBoundedChannel<SocketEvent>(capacity: 1)
        #expect(await channel.send(SocketEvent(topic: "one", payload: Data())))
        await expectCanceledFullSendSettles(channel, value: SocketEvent(topic: "two", payload: Data()))
    }

    @Test func canceledUploadProducerDoesNotStrandWhenFull() async {
        let channel = SocketBoundedChannel<SocketRequestChunk>(capacity: 1)
        #expect(await channel.send(chunk(0)))
        await expectCanceledFullSendSettles(channel, value: chunk(1))
    }

    @Test func lifecycleQueueKeepsLatestBeforeConsumption() async throws {
        let channel = SocketBoundedChannel<Data>(capacity: 1)
        #expect(await channel.offerLatest(Data("one".utf8)))
        #expect(await channel.offerLatest(Data("two".utf8)))

        #expect(try await channel.next(onCancel: {}) == Data("two".utf8))
        await channel.discard()
    }

    private func expectCanceledFullSendSettles<Element: Sendable>(
        _ channel: SocketBoundedChannel<Element>,
        value: Element
    ) async {
        let producer = Task { await channel.send(value) }
        await Task.yield()
        producer.cancel()
        #expect(await producer.value == false)
    }

    private func chunk(_ sequence: UInt32) -> SocketRequestChunk {
        SocketRequestChunk(sequence: sequence, payload: Data([UInt8(sequence)]), end: false)
    }
}
