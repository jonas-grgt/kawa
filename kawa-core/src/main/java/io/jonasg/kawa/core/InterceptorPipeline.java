package io.jonasg.kawa.core;

import java.util.List;

public final class InterceptorPipeline {

    private final List<Interceptor> interceptors;

    public InterceptorPipeline(List<Interceptor> interceptors) {
        this.interceptors = List.copyOf(interceptors);
    }

    public void onRequest(
            GatewayContext context,
            Request request
    ) {
        for (Interceptor interceptor : interceptors) {
            if (interceptor.appliesToRequest(request)) {
                interceptor.onRequest(context, request);
                if (context.isShortCircuited()) {
                    return;
                }
            }
        }
    }

    public void onResponse(
            GatewayContext context,
            Response response
    ) {
        for (Interceptor interceptor : interceptors) {
            if (interceptor.appliesToResponse(response)) {
                interceptor.onResponse(context, response);
            }
        }
    }
}
