plugins {
    id("org.springframework.boot") version "4.1.1" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1" apply false
}

allprojects {
    group = "com.fabioqmarsiaj"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
        maven {
            name = "confluent"
            url = uri("https://packages.confluent.io/maven/")
        }
    }
}

subprojects {
    plugins.withType<JavaPlugin> {
        configure<JavaPluginExtension> {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(21))
            }
        }

        tasks.withType<Test> {
            useJUnitPlatform()
        }
    }
}
