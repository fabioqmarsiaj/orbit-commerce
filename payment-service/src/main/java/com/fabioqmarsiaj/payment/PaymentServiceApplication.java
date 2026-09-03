package com.fabioqmarsiaj.payment;

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
@SpringBootApplication(scanBasePackages = {"com.fabioqmarsiaj.payment", "com.fabioqmarsiaj.outbox"})
@EnableScheduling
@EntityScan(basePackages = {"com.fabioqmarsiaj.payment.persistence", "com.fabioqmarsiaj.outbox.persistence"})
@EnableJpaRepositories(basePackages = {"com.fabioqmarsiaj.payment.persistence", "com.fabioqmarsiaj.outbox.persistence"})
public class PaymentServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(PaymentServiceApplication.class, args);
	}

}
