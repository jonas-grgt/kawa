package io.jonasg.kawa.http;

import io.jonasg.kawa.governance.TopicSpec;

import java.util.ArrayList;
import java.util.List;

/// In-memory [TopicAdmin] for handler tests: records create/delete calls instead of talking
/// to a broker.
final class FakeTopicAdmin implements TopicAdmin {

    final List<TopicSpec> created = new ArrayList<>();
    final List<String> deleted = new ArrayList<>();
    RuntimeException createError;

    @Override
    public void createTopic(TopicSpec spec) {
        if (createError != null) {
            throw createError;
        }
        created.add(spec);
    }

    @Override
    public void deleteTopic(String name) {
        deleted.add(name);
    }

    @Override
    public void close() {
    }
}