plugins {
    `java-library`
    id("io.spring.dependency-management")
}

// Not a runnable Spring Boot application (no org.springframework.boot
// plugin applied) — this is a library module consumed by order-service,
// inventory-service, and (eventually) payment-service/shipping-service.
// We still need the Spring Boot BOM so the Spring Data JPA / Spring Kafka
// versions used here exactly match what each consuming service's own
// spring-boot-starter-* dependencies resolve to; without it, Gradle would
// pick whatever transitive versions come first, which could silently
// diverge from what order-service/inventory-service actually run with.
dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:4.1.1")
    }
}

dependencies {
    // `api` (not `implementation`): consuming services need these types
    // (OutboxEntity, KafkaTemplate, etc.) visible on their own compile
    // classpath, since AbstractOutboxPublisher/OutboxRecorder's public
    // signatures expose them directly.
    api("org.springframework.data:spring-data-jpa")
    // spring-data-jpa alone doesn't pull in the JPA API itself the way
    // spring-boot-starter-data-jpa's own dependency graph does — needed
    // explicitly here since this module can't apply the Spring Boot
    // plugin (it's a library, not a runnable application).
    api("jakarta.persistence:jakarta.persistence-api")
    api("org.springframework.kafka:spring-kafka")
    api("org.apache.avro:avro:1.11.3")
    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")
}
