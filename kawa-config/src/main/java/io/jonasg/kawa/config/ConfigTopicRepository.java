package io.jonasg.kawa.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import java.util.Properties;
import java.util.function.UnaryOperator;

/// Writes full [GatewayConfig] snapshots to the compacted gateway-config topic. The consumer
/// side ([ConfigTopicConsumer]) picks each snapshot up and applies it through the normal
/// [io.jonasg.kawa.server.DynamicConfigManager] flow, so a write here is a live config change.
///
/// Every snapshot is sent with the same key so compaction keeps only the latest one. [upsert]
/// blocks until the broker acknowledges, so a REST caller knows the change is persisted before
/// it returns. As a [GatewayConfigRepository] it reports the newest *persisted* snapshot via
/// [getActiveConfig] - the base the admin API's read-modify-write must build on, because the consumer
/// applies snapshots asynchronously.
public final class ConfigTopicRepository implements GatewayConfigRepository, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConfigTopicRepository.class);

    private static final String CONFIG_KEY = "config";

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final JsonMapper mapper = JsonMapper.builder().build();

    private volatile GatewayConfig lastWritten;

    /// @param bootstrapServers `host:port` list of the cluster hosting the config topic
    /// @param topic the config topic name
    /// @param extraProps extra producer properties (e.g. SASL/security settings for the
    ///                   config topic) on top of the base bootstrap/serializer configuration
    public ConfigTopicRepository(String bootstrapServers, String topic, Properties extraProps) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(producerProps(bootstrapServers, extraProps));
    }

    /// The newest snapshot this repository has persisted, or `null` before the first write.
    @Override
    public GatewayConfig getActiveConfig() {
        return lastWritten;
    }

    /// Writes a full config snapshot to the topic, blocking until the broker acknowledges.
    @Override
    public void upsert(GatewayConfig config) {
        try {
            String json = serialize(config);
            producer.send(new ProducerRecord<>(topic, CONFIG_KEY, json)).get();
            lastWritten = config;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while writing config snapshot to topic " + topic, e);
        } catch (Exception e) {
            throw new IllegalStateException("failed to write config snapshot to topic " + topic, e);
        }
    }

    /// Applies [mutation] to the newest persisted snapshot (or an empty config before the
    /// first write) and persists the result.
    @Override
    public void update(UnaryOperator<GatewayConfig> mutation) {
        upsert(mutation.apply(getActiveConfigOrEmpty()));
    }

    /// Serializes a snapshot to the JSON form the consumer deserializes. Package-private so the
    /// message-encoding path is testable without a broker.
    String serialize(GatewayConfig config) {
        try {
            return mapper.writeValueAsString(config);
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize config snapshot", e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }

    private static Properties producerProps(String bootstrapServers, Properties extraProps) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putAll(extraProps);
        return props;
    }
}