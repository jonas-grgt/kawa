package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.core.GatewayContext;
import org.apache.kafka.common.message.FindCoordinatorResponseData;

public final class FindCoordinatorVirtualTopicTransform implements VirtualTopicTransform<Object, FindCoordinatorResponseData> {

    private final AdvertisedListener advertised;

    public FindCoordinatorVirtualTopicTransform(AdvertisedListener advertised) {
        this.advertised = advertised;
    }

    @Override
    public short apiKey() {
        return 10;
    }

    @Override
    public void onRequest(
            GatewayContext context,
            Object body
    ) {
        // no-op
    }

    @Override
    public void onResponse(
            GatewayContext context,
            FindCoordinatorResponseData data
    ) {
        data.setHost(advertised.host());
        data.setPort(advertised.port());
        for (FindCoordinatorResponseData.Coordinator coordinator : data.coordinators()) {
            coordinator.setHost(advertised.host());
            coordinator.setPort(advertised.port());
        }
    }
}