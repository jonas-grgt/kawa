package io.jonasg.kawa.it;

import io.jonasg.kassert.Kassertions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Exercises the Kafka assertion DSL (`filter`, `anySatisfy`, `allSatisfy`,
/// `noneSatisfy`) against a passthrough gateway with no virtual topics. Each test uses
/// its own single-partition topic so records never leak across tests.
class KafkaAssertionDslIT extends GatewayTestSupport {

	private static final AtomicInteger TOPIC_SEQ = new AtomicInteger();

	private final List<KafkaConsumer<String, String>> consumers = new ArrayList<>();

	@Override
	protected String groupId() {
		return "kafka-assertion-dsl-it";
	}

	@Override
	protected Map<String, String> virtualTopics() {
		return Map.of();
	}

	@AfterEach
	void closeConsumers() {
		consumers.forEach(KafkaConsumer::close);
		consumers.clear();
	}

	@Test
	void anySatisfyPassesWhenRecordAlreadyPresent() {
		String topic = newTopic();
		produce(topic, "k", "v1");

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
	}

	@Test
	void anySatisfyPassesWhenRecordArrivesAfterStart() {
		String topic = newTopic();
		produceAsync(topic, "k", "v1", 500);

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
	}

	@Test
	void anySatisfyPassesWhenOnlyOneOfManySatisfies() {
		String topic = newTopic();
		produce(topic, "k1", "v1");
		produce(topic, "k2", "v2");

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.anySatisfy(rec -> {
					assertThat(rec.key()).isEqualTo("k2");
					assertThat(rec.value()).isEqualTo("v2");
				});
	}

	@Test
	void anySatisfyTimesOutWhenNoRecordArrives() {
		String topic = newTopic();

		assertThatThrownBy(() -> Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(2, TimeUnit.SECONDS)
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1")))
				.isInstanceOf(AssertionError.class)
				.hasMessageContaining("none did")
				.hasMessageContaining("Matching records observed");
	}

	@Test
	void anySatisfyTimesOutWhenNoRecordMatchesFilter() {
		String topic = newTopic();
		produce(topic, "k1", "v1");

		assertThatThrownBy(() -> Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(2, TimeUnit.SECONDS)
				.filter(rec -> rec.key().equals("k2"))
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1")))
				.isInstanceOf(AssertionError.class)
				.hasMessageContaining("none did");
	}

	@Test
	void allSatisfyPassesWhenAllRecordsSatisfy() {
		String topic = newTopic();
		produce(topic, "k1", "v1");
		produce(topic, "k2", "v1");

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.allSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
	}

	@Test
	void allSatisfyPassesWhenRecordArrivesAfterStart() {
		String topic = newTopic();
		produceAsync(topic, "k", "v1", 500);

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.allSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
	}

	@Test
	void allSatisfyTimesOutWhenOneRecordViolates() {
		String topic = newTopic();
		produce(topic, "k1", "v1");
		produce(topic, "k2", "v2");

		assertThatThrownBy(() -> Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(2, TimeUnit.SECONDS)
				.allSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1")))
				.isInstanceOf(AssertionError.class)
				.hasMessageContaining("Expected every matching record")
				.hasMessageContaining("Matching records observed");
	}

	@Test
	void allSatisfyFailsWhenNoRecordMatchesFilter() {
		String topic = newTopic();
		produce(topic, "k", "v-other");

		assertThatThrownBy(() -> Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(2, TimeUnit.SECONDS)
				.filter(rec -> rec.value().equals("v1"))
				.allSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1")))
				.isInstanceOf(AssertionError.class)
				.hasMessageContaining("no records matched the filter");
	}

	@Test
	void allSatisfyKeepsPollingAfterFirstViolation() {
		String topic = newTopic();
		produceAsync(topic, "k1", "v-bad", 300);
		produceAsync(topic, "k2", "v-good", 900);

		assertThatThrownBy(() -> Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.allSatisfy(rec -> assertThat(rec.value()).isEqualTo("v-good")))
				.isInstanceOf(AssertionError.class)
				.hasMessageContaining("did not");
	}

	@Test
	void noneSatisfyPassesWhenNoRecordsArrive() {
		String topic = newTopic();

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(2, TimeUnit.SECONDS)
				.noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
	}

	@Test
	void noneSatisfyPassesWhenRecordsArriveButNoneSatisfy() {
		String topic = newTopic();
		produce(topic, "k", "v1");

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(2, TimeUnit.SECONDS)
				.noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v2"));
	}

	@Test
	void noneSatisfyFailsFastWhenRecordAlreadySatisfies() {
		String topic = newTopic();
		produce(topic, "k", "v-bad");

		assertThatThrownBy(() -> Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v-bad")))
				.isInstanceOf(AssertionError.class)
				.hasMessageContaining("Expected no matching record");
	}

	@Test
	void noneSatisfyFailsWhenViolatingRecordArrivesLater() {
		String topic = newTopic();
		produceAsync(topic, "k", "v-bad", 500);

		assertThatThrownBy(() -> Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v-bad")))
				.isInstanceOf(AssertionError.class)
				.hasMessageContaining("Expected no matching record");
	}

	@Test
	void noneSatisfyPassesWithFilter() {
		String topic = newTopic();
		produce(topic, "k1", "v1");

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(2, TimeUnit.SECONDS)
				.filter(rec -> rec.key().equals("k2"))
				.noneSatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
	}

	@Test
	void filterRestrictsToMatchingRecords() {
		String topic = newTopic();
		produce(topic, "k1", "v1");
		produce(topic, "k2", "v2");

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.filter(rec -> rec.key().equals("k1"))
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.filter(rec -> rec.key().equals("k2"))
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v2"));
	}

	@Test
	void filterChainingCombinesWithAnd() {
		String topic = newTopic();
		produce(topic, "k", "v-good");
		produce(topic, "k", "v-bad");

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.filter(rec -> rec.key().equals("k"))
				.filter(rec -> rec.value().equals("v-good"))
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v-good"));

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.filter(rec -> rec.key().equals("k"))
				.filter(rec -> rec.value().equals("v-bad"))
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v-bad"));
	}

	@Test
	void filterAppliedBeforeAsyncArrival() {
		String topic = newTopic();
		produceAsync(topic, "k", "v1", 400);

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.within(3, TimeUnit.SECONDS)
				.filter(rec -> rec.key().equals("k"))
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
	}

	@Test
	void omitsFromBeginningAndWithin_readsNewlyArrivingRecords() {
		String topic = newTopic();
		produceAsync(topic, "k", "v1", 400);

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
	}

	@Test
	void usesFromBeginningWithoutWithin() {
		String topic = newTopic();
		produce(topic, "k", "v1");

		Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.fromBeginning()
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1"));
	}

	@Test
	void defaultTimeoutAppliesWhenWithinOmitted() {
		String topic = newTopic();

		assertThatThrownBy(() -> Kassertions.consume(newConsumer())
				.assignedTo(topic, 0)
				.anySatisfy(rec -> assertThat(rec.value()).isEqualTo("v1")))
				.isInstanceOf(AssertionError.class)
				.hasMessageContaining("none did");
	}

	private String newTopic() {
		String name = "dsl-" + TOPIC_SEQ.incrementAndGet();
		try {
			brokerAdmin.createTopics(List.of(new NewTopic(name, 1, (short) 1))).all()
					.get(10, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new RuntimeException(e);
		}
		return name;
	}

	private KafkaConsumer<String, String> newConsumer() {
		Properties props = saslProps(gatewayBootstrap);
		props.put(GROUP_ID_CONFIG, groupId() + "-" + System.nanoTime());
		props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumers.add(consumer);
		return consumer;
	}

	private void produce(String topic, String key, String value) {
		try {
			gatewayProducer.send(new ProducerRecord<>(topic, key, value)).get(5, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new RuntimeException(e);
		}
	}

	private void produceAsync(String topic, String key, String value, long delayMillis) {
		CompletableFuture.runAsync(() -> {
			try {
				Thread.sleep(delayMillis);
				produce(topic, key, value);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
	}
}
