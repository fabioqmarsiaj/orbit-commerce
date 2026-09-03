package com.fabioqmarsiaj.inventory;

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
 * cover {@code com.fabioqmarsiaj.outbox} — see
 * {@code OrderServiceApplication}'s identical setup (in order-service) for
 * the full explanation of why this is needed.
 */
@SpringBootApplication(scanBasePackages = {"com.fabioqmarsiaj.inventory", "com.fabioqmarsiaj.outbox"})
@EnableScheduling
@EntityScan(basePackages = {"com.fabioqmarsiaj.inventory.persistence", "com.fabioqmarsiaj.outbox.persistence"})
@EnableJpaRepositories(basePackages = {"com.fabioqmarsiaj.inventory.persistence", "com.fabioqmarsiaj.outbox.persistence"})
public class InventoryServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(InventoryServiceApplication.class, args);
	}

}
