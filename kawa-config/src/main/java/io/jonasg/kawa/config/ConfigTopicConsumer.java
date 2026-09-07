package io.jonasg.kawa.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/// Consumes the compacted gateway-config topic and delivers each full [GatewayConfig]
/// snapshot to a callback.
///
/// On [start] the consumer reads the topic from the beginning until it has caught up to the
/// end offset that existed at assignment time (the "initial load"), then continues consuming
/// live changes. [awaitInitialLoad] blocks until that catch-up is done, so the gateway can
/// refuse to serve until it has applied the config that existed at boot.
///
/// A single bad message (unparseable JSON, or a callback that rejects the config) is logged
/// and skipped rather than killing the consumer thread - the gateway keeps its last-known-good
/// config and the next valid message resumes the stream.
///
/// The topic is expected to have a single partition and `cleanup.policy=compact`; ordering
/// within the partition is what makes "apply the last snapshot" correct.
public final class ConfigTopicConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConfigTopicConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final Consumer<GatewayConfig> onConfig;
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final CountDownLatch initialLoad = new CountDownLatch(1);

    private volatile boolean running;
    private Thread thread;

    /// @param bootstrapServers `host:port` list of the cluster hosting the config topic
    /// @param topic the config topic name
    /// @param groupId consumer group id (only one consumer in the group is active on the
    ///                single partition; the rest are standby)
    /// @param onConfig invoked for every valid config snapshot, in offset order
    public ConfigTopicConsumer(
            String bootstrapServers,
            String topic,
            String groupId,
            Consumer<GatewayConfig> onConfig
    ) {
        this(bootstrapServers, topic, groupId, new Properties(), onConfig);
    }

    /// Variant that accepts extra consumer properties (e.g. SASL/security settings for the
    /// config topic) on top of the base bootstrap/group/deserializer configuration.
    public ConfigTopicConsumer(
            String bootstrapServers,
            String topic,
            String groupId,
            Properties extraProps,
            Consumer<GatewayConfig> onConfig
    ) {
        this.topic = topic;
        this.onConfig = onConfig;
        this.consumer = new KafkaConsumer<>(consumerProps(bootstrapServers, groupId, extraProps));
    }

    /// Starts the consumer thread. Idempotent.
    public void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::run, "kawa-config-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    /// Blocks until the initial catch-up is complete, i.e. every config message that existed
    /// at assignment time has been delivered to the callback.
    public void awaitInitialLoad() throws InterruptedException {
        initialLoad.await();
    }

    /// Whether the initial catch-up has completed.
    public boolean initialLoadComplete() {
        return initialLoad.getCount() == 0;
    }

    /// Stops the consumer thread and closes the underlying client. Idempotent; safe to call
    /// before [start] (the client is closed without a thread having run).
    @Override
    public void close() {
        running = false;
        consumer.wakeup();
        if (thread != null) {
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        consumer.close();
    }

    private void run() {
        try {
            consumer.subscribe(List.of(topic));
            TopicPartition partition = awaitAssignment();
            consumer.seekToBeginning(Set.of(partition));
            long targetEnd = consumer.endOffsets(Set.of(partition)).get(partition);
            boolean caughtUp = false;
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    handle(record);
                }
                if (!caughtUp && consumer.position(partition) >= targetEnd) {
                    caughtUp = true;
                    initialLoad.countDown();
                    log.info("initial config load complete for topic {} at offset {}", topic, targetEnd);
                }
            }
        } catch (WakeupException e) {
            // expected on close()
        } catch (Exception e) {
            log.error("config consumer for topic {} failed", topic, e);
        } finally {
            consumer.close();
        }
    }

    private TopicPartition awaitAssignment() {
        while (running) {
            Set<TopicPartition> assignment = consumer.assignment();
            if (!assignment.isEmpty()) {
                return assignment.iterator().next();
            }
            consumer.poll(Duration.ofMillis(100));
        }
        throw new IllegalStateException("config consumer stopped before partition assignment");
    }

    /// Deserializes one message and delivers it to the callback. Package-private so the
    /// message-handling path is testable without a broker.
    void handle(ConsumerRecord<String, String> record) {
        try {
            GatewayConfig config = mapper.readValue(record.value(), GatewayConfig.class);
            onConfig.accept(config);
        } catch (Exception e) {
            log.error("skipping invalid config message at offset {} on topic {}", record.offset(), topic, e);
        }
    }

    private static Properties consumerProps(
            String bootstrapServers,
            String groupId,
            Properties extraProps
    ) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putAll(extraProps);
        return props;
    }
}
