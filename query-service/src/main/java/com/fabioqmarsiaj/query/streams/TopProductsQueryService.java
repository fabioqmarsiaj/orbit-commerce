package com.fabioqmarsiaj.query.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Interactively queries the {@code product-view-counts} windowed state
 * store (materialized by {@link UserActivityStreamsConfig}) for
 * {@code GET /analytics/top-products}.
 *
 * <p>Deliberately queries only the CURRENT (most recent, still-open)
 * 1-minute tumbling window — the most literal reading of TASKS.md's
 * Phase 7 wording ("tumbling window (1 min) ... count via state store"),
 * and simpler to implement/explain than summing several recent windows.
 * The window's start is computed by truncating "now" down to the
 * configured window size, mirroring exactly how Kafka Streams itself
 * assigns records to tumbling windows.
 */
@Service
public class TopProductsQueryService {

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public TopProductsQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    /**
     * @return the top {@code limit} products by view count in the
     *         current tumbling window, descending. Returns an empty list
     *         if the store isn't ready yet or no views have been counted
     *         in the current window — this is expected/normal until
     *         ingestion-service (Phase 8) actually produces
     *         {@code ProductViewed} events onto {@code user-activity.events}.
     */
    public List<ProductViewCount> topProducts(int limit) {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            return List.of();
        }

        ReadOnlyWindowStore<String, Long> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        UserActivityStreamsConfig.PRODUCT_VIEW_COUNTS_STORE,
                        QueryableStoreTypes.windowStore()));

        long windowSizeMillis = WINDOW_SIZE.toMillis();
        long currentWindowStart = (Instant.now().toEpochMilli() / windowSizeMillis) * windowSizeMillis;
        Instant windowStart = Instant.ofEpochMilli(currentWindowStart);
        Instant windowEnd = windowStart.plus(WINDOW_SIZE);

        Map<String, Long> counts = new HashMap<>();
        try (KeyValueIterator<Windowed<String>, Long> iterator = store.fetchAll(windowStart, windowEnd)) {
            while (iterator.hasNext()) {
                KeyValue<Windowed<String>, Long> entry = iterator.next();
                counts.merge(entry.key.key(), entry.value, Long::sum);
            }
        }

        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(limit)
                .map(e -> new ProductViewCount(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    public record ProductViewCount(String productId, long viewCount) {
    }
}
