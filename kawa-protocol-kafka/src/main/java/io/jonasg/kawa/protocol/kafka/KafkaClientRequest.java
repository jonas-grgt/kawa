package io.jonasg.kawa.protocol.kafka;

import io.jonasg.kawa.core.Request;
import io.netty.buffer.ByteBuf;

/// A Kafka request as seen by the gateway.
///
/// Either `body` (decoded POJO) or `rawBody` (retained `ByteBuf` slice
/// of the original frame) is set, never both. The holder of a request with a raw body is
/// responsible for releasing the buffer.
public record KafkaClientRequest(KafkaHeader header, Object body, ByteBuf rawBody) implements Request {

    @Override
    public int apiKey() {
        return header.apiKey();
    }

    @Override
    public String apiName() {
        return header.apiName();
    }

    @Override
    public short apiVersion() {
        return header.apiVersion();
    }

    @Override
    public int correlationId() {
        return header.correlationId();
    }

    @Override
    public String clientId() {
        return header.clientId();
    }

    public static KafkaClientRequest of(
            KafkaHeader header,
            Object body
    ) {
        return new KafkaClientRequest(header, body, null);
    }
}
