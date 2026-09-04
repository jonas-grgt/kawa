package io.jonasg.kawa.protocol.kafka;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.Map;

/// Builds the ApiVersions response the gateway answers itself with. It advertises the
/// gateway's decode ranges intersected with what the broker supports, so clients never use
/// a version the broker cannot handle.
public final class ApiVersionsResponseBuilder {

    public static final short MAX_SUPPORTED_VERSION = 3;

    private final SupportedVersions supported;

    public ApiVersionsResponseBuilder(SupportedVersions supported) {
        this.supported = supported;
    }

    public ApiVersionsResult build(
            short requestVersion,
            Map<Short, VersionRange> brokerRanges
    ) {
        short responseVersion = (short) Math.min(requestVersion, MAX_SUPPORTED_VERSION);
        int errorCode = requestVersion > MAX_SUPPORTED_VERSION ? Errors.UNSUPPORTED_VERSION.code() : 0;

        Map<Short, VersionRange> base = brokerRanges == null || brokerRanges.isEmpty()
                ? supported.decodeRanges()
                : brokerRanges;

        ApiVersionsResponseData.ApiVersionCollection apiKeys =
                new ApiVersionsResponseData.ApiVersionCollection();
        base.forEach((apiKey, brokerRange) -> {
            VersionRange advertised = supported.advertised(apiKey, brokerRange);
            if (advertised != null) {
                apiKeys.add(new ApiVersionsResponseData.ApiVersion()
                        .setApiKey(apiKey)
                        .setMinVersion(advertised.min())
                        .setMaxVersion(advertised.max()));
            }
        });

        var data = new ApiVersionsResponseData()
                .setErrorCode((short) errorCode)
                .setApiKeys(apiKeys)
                .setThrottleTimeMs(0);

        return new ApiVersionsResult(data, responseVersion);
    }

    public record ApiVersionsResult(ApiVersionsResponseData data, short responseVersion) {
    }
}
