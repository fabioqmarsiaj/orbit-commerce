package com.fabioqmarsiaj.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * {@code @EnableScheduling} activates Spring's {@code @Scheduled} task
 * support, required by {@code OutboxPublisher}'s polling loop.
 *
 * <p>{@code scanBasePackages}/{@code @EntityScan}/{@code @EnableJpaRepositories}
 * explicitly widen Spring's component/entity/repository scanning to also
 * cover {@code com.fabioqmarsiaj.outbox} — the shared {@code outbox-support}
 * module's package. By default {@code @SpringBootApplication} only scans
 * the package this class lives in (and subpackages), so without this,
 * {@code OutboxEntity}/{@code OutboxRepository}/{@code OutboxRecorder} from
 * that module would never be picked up (the app would fail to start with
 * {@code NoSuchBeanDefinitionException}, or the {@code outbox} table would
 * never get created). See {@code docs/decisions.md} for the extraction
 * writeup this is part of.
 */
@SpringBootApplication(scanBasePackages = {"com.fabioqmarsiaj.order", "com.fabioqmarsiaj.outbox"})
@EnableScheduling
@EntityScan(basePackages = {"com.fabioqmarsiaj.order.persistence", "com.fabioqmarsiaj.outbox.persistence"})
@EnableJpaRepositories(basePackages = {"com.fabioqmarsiaj.order.persistence", "com.fabioqmarsiaj.outbox.persistence"})
public class OrderServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderServiceApplication.class, args);
	}

}
