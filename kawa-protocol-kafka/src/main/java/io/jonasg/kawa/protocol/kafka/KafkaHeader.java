package io.jonasg.kawa.protocol.kafka;

import org.apache.kafka.common.protocol.ApiKeys;

public record KafkaHeader(short apiKey, short apiVersion, int correlationId, String clientId) {

    public String apiName() {
        ApiKeys api = ApiKeys.forId(apiKey);
        return api == null ? "UNKNOWN(" + apiKey + ")" : api.name;
    }

    /// Request header version (v1 = classic, v2 = flexible) for this api key + version.
    public short requestHeaderVersion() {
        ApiKeys api = ApiKeys.forId(apiKey);
        return api == null ? 1 : api.requestHeaderVersion(apiVersion);
    }

    public short responseHeaderVersion() {
        ApiKeys api = ApiKeys.forId(apiKey);
        return api == null ? 0 : api.responseHeaderVersion(apiVersion);
    }

    public static KafkaHeader of(
            short apiKey,
            short apiVersion,
            int correlationId,
            String clientId
    ) {
        return new KafkaHeader(apiKey, apiVersion, correlationId, clientId);
    }
}
