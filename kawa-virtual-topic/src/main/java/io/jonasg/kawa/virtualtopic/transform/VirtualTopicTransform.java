package io.jonasg.kawa.virtualtopic.transform;

import io.jonasg.kawa.core.ApiTransform;

/// Virtual-topic gateway transform. Per-request rewrite state is obtained from the context via
/// [VirtualTopicState.from].
///
/// @param <Req>  the decoded request body type
/// @param <Resp> the decoded response body type
public interface VirtualTopicTransform<Req, Resp> extends ApiTransform<Req, Resp> {
}
