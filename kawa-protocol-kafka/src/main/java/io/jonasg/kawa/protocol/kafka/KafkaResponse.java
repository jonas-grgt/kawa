package io.jonasg.kawa.protocol.kafka;

import io.jonasg.kawa.core.Response;
import io.netty.buffer.ByteBuf;

/// A Kafka response about to be sent back to a client.
///
/// Either `body` (decoded POJO) or `rawBody` (retained `ByteBuf` slice)
/// is set, never both. The holder of a response with a raw body is responsible for releasing
/// the buffer.
public final class KafkaResponse implements Response {

    private final KafkaHeader header;
    private final Object body;
    private final ByteBuf rawBody;

    public KafkaResponse(
            KafkaHeader header,
            Object body,
            ByteBuf rawBody
    ) {
        this.header = header;
        this.body = body;
        this.rawBody = rawBody;
    }

    public KafkaHeader header() {
        return header;
    }

    @Override
    public Object body() {
        return body;
    }

    /// Raw body bytes for responses passed through without inspection, or `null`.
    public ByteBuf rawBody() {
        return rawBody;
    }

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

    public static KafkaResponse of(
            KafkaHeader header,
            Object body
    ) {
        return new KafkaResponse(header, body, null);
    }
}
