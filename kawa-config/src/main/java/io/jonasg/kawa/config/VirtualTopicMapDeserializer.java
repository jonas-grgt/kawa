package io.jonasg.kawa.config;

import tools.jackson.core.JsonParser;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;

import java.util.LinkedHashMap;
import java.util.Map;

/// Deserializes the `virtualTopics` configuration section.
///
/// Both of these YAML shapes are accepted:
///
/// ```yaml
/// virtualTopics:
///   orders: orders-v2
///   customers:
///     topic: crm.customers
///     exposePhysicalTopic: true
/// ```
public final class VirtualTopicMapDeserializer extends ValueDeserializer<Map<String, VirtualTopicConfig>> {

    private static final TypeReference<Map<String, JsonNode>> RAW =
            new TypeReference<>() {};

    @Override
    public Map<String, VirtualTopicConfig> deserialize(
            JsonParser parser,
            DeserializationContext context
    ) {
        Map<String, JsonNode> raw = parser.readValueAs(RAW);
        Map<String, VirtualTopicConfig> result = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> entry : raw.entrySet()) {
            JsonNode value = entry.getValue();
            if (value.isString()) {
                result.put(entry.getKey(), new VirtualTopicConfig(value.stringValue()));
            } else if (value.isObject()) {
                result.put(entry.getKey(), fromObject(entry.getKey(), value, context));
            } else {
                throw new IllegalArgumentException(
                        "Invalid virtual topic mapping for topic '" + entry.getKey()
                                + "': expected a topic name string or a map with a 'topic' key");
            }
        }
        return Map.copyOf(result);
    }

    private static VirtualTopicConfig fromObject(
            String logicalTopic,
            JsonNode object,
            DeserializationContext context
    ) {
        JsonNode topicNode = object.get("topic");
        if (topicNode == null || !topicNode.isString()) {
            throw new IllegalArgumentException(
                    "Invalid virtual topic mapping for topic '" + logicalTopic
                            + "': expected a topic name string or a map with a 'topic' key");
        }
        VirtualTopicFilterConfig filter = parseFilter(logicalTopic, object.get("filter"), context);
        boolean exposePhysicalTopic = parseExposePhysicalTopic(logicalTopic, object.get("exposePhysicalTopic"));
        return new VirtualTopicConfig(topicNode.stringValue(), filter, exposePhysicalTopic);
    }

    private static boolean parseExposePhysicalTopic(
            String logicalTopic,
            JsonNode node
    ) {
        if (node == null || node.isNull()) {
            return false;
        }
        if (!node.isBoolean()) {
            throw new IllegalArgumentException(
                    "Invalid virtual topic mapping for topic '" + logicalTopic
                            + "': 'exposePhysicalTopic' must be a boolean");
        }
        return node.booleanValue();
    }

    /// Delegates the `filter` node to Jackson's polymorphic `type`-based dispatch
    /// ([VirtualTopicFilterConfig]'s `@JsonTypeInfo`/`@JsonSubTypes`), so each
    /// filter kind is deserialized directly into its own concrete config record.
    private static VirtualTopicFilterConfig parseFilter(
            String logicalTopic,
            JsonNode filterNode,
            DeserializationContext context
    ) {
        if (filterNode == null || filterNode.isNull()) {
            return null;
        }
        if (!filterNode.isObject()) {
            throw new IllegalArgumentException(
                    "Invalid filter config for virtual topic '" + logicalTopic
                            + "': expected an object with a required 'type' key");
        }
        try {
            return context.readTreeAsValue(filterNode, VirtualTopicFilterConfig.class);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Invalid filter config for virtual topic '" + logicalTopic + "': " + e.getMessage(), e);
        }
    }
}
