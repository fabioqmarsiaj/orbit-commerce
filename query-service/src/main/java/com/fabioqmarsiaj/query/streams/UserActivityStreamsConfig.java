package com.fabioqmarsiaj.query.streams;

import com.fabioqmarsiaj.query.messaging.KafkaTopics;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;

import java.time.Duration;

/**
 * Kafka Streams topology (Phase 7, Part B): counts {@code ProductViewed}
 * events per {@code productId} in 1-minute tumbling windows, materialized
 * into a named state store that {@code TopProductsQueryService} queries
 * interactively for {@code GET /analytics/top-products}.
 *
 * <p>{@code @EnableKafkaStreams} registers the {@code defaultKafkaStreamsBuilder}
 * {@code StreamsBuilderFactoryBean} — Spring Boot's Kafka Streams
 * autoconfiguration (`KafkaStreamsAnnotationDrivenConfiguration`, in
 * `spring-boot-kafka`) is conditional on that exact bean existing, and
 * builds the required {@code KafkaStreamsConfiguration} bean automatically
 * from the {@code spring.kafka.streams.*} properties (see
 * {@code application.properties}) — no manual
 * {@code new KafkaStreamsConfiguration(Map.of(...))} bean needed here,
 * unlike what older Spring Kafka Streams examples/tutorials show.
 *
 * <p>{@code user-activity.events} carries three different Avro types
 * (`ProductViewed`/`AddedToCart`/`SearchPerformed`) sharing one topic —
 * the same multi-type-topic shape as every other topic in this project.
 * The stream reads them all as {@link GenericRecord} (via
 * {@code GenericAvroSerde}, configured as the default value serde) and
 * filters down to {@code ProductViewed} by inspecting
 * {@code record.getSchema().getName()} — the Kafka-Streams-side
 * equivalent of the {@code eventType} discriminator string pattern used
 * everywhere else in this project (outbox rows, order-service's event
 * store, this same service's own {@code timeline_entries} table).
 */
@Configuration
@EnableKafkaStreams
public class UserActivityStreamsConfig {

    /**
     * Name of the interactively-queryable state store backing the
     * windowed product-view counts. Referenced by
     * {@code TopProductsQueryService} when calling
     * {@code KafkaStreams#store(...)}.
     */
    public static final String PRODUCT_VIEW_COUNTS_STORE = "product-view-counts";

    /**
     * Tumbling window size — matches the "1 min" wording in TASKS.md's
     * Phase 7 task description. {@code ofSizeWithNoGrace} (as opposed to
     * {@code ofSizeAndGrace}) means a window closes immediately at its
     * boundary with no allowance for late-arriving records — the
     * simplest possible windowing semantics, appropriate for a
     * best-effort analytics feature rather than a correctness-critical
     * one.
     */
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);

    /**
     * Declares {@code user-activity.events} as a Spring-managed
     * {@link NewTopic} bean — Spring Boot's {@code KafkaAdmin} picks up
     * every {@code NewTopic} bean in the context and idempotently
     * ensures it exists (a no-op against the real broker, where
     * {@code infra/kafka/init-topics.sh} already created all 8 topics;
     * relevant here because this project's other {@code KafkaTopics}
     * classes never needed to declare topics this way — every other
     * service only ever uses {@code @KafkaListener}, whose consumer
     * simply waits/retries gracefully if a topic doesn't exist yet).
     * Kafka Streams behaves very differently: with
     * {@code allow.auto.create.topics=false} (the correct setting for
     * everything else in this project — see docs/decisions.md), a
     * Streams topology whose SOURCE topic doesn't exist throws
     * {@code MissingSourceTopicException} and leaves the whole
     * {@code StreamThread} permanently in the {@code ERROR} state — not
     * a transient, retried condition like a plain consumer would treat
     * it. This was discovered running query-service's own test suite:
     * the ephemeral Testcontainers-managed broker used for tests starts
     * completely empty (no topics pre-created, unlike the real
     * `docker-compose` broker), so without this bean the Streams thread
     * would immediately and permanently fail every time the test
     * context loads.
     */
    @Bean
    public NewTopic userActivityEventsTopic() {
        return TopicBuilder.name(KafkaTopics.USER_ACTIVITY_EVENTS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public KTable<Windowed<String>, Long> productViewCounts(StreamsBuilder streamsBuilder) {
        KStream<String, GenericRecord> activity = streamsBuilder.stream(KafkaTopics.USER_ACTIVITY_EVENTS);

        return activity
                .filter((key, value) -> value != null && "ProductViewed".equals(value.getSchema().getName()))
                // GenericRecord#get returns org.apache.avro.util.Utf8 for
                // "string"-typed fields by default, NOT java.lang.String -
                // a direct (String) cast throws ClassCastException at
                // runtime (compiles fine, since the cast is unchecked at
                // compile time). .toString() works for both Utf8 and
                // String, so this stays correct even if the value serde's
                // string handling ever changes. Found live via real
                // ingestion-service (Phase 8) traffic - Part B had
                // previously only ever been verified by compilation, with
                // no real user-activity.events producer to catch this.
                // See docs/decisions.md ("Phase 8 - ingestion-service").
                .map((key, value) -> KeyValue.pair(value.get("productId").toString(), value))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
                .count(Named.as(PRODUCT_VIEW_COUNTS_STORE + "-count"),
                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(PRODUCT_VIEW_COUNTS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()));
    }
}
