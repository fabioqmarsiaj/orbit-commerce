# Phase 9: shipping-service container image.
#
# Build context MUST be the repo root, not this directory — this is a
# Gradle multi-module monorepo and shipping-service depends on both
# :event-schemas and :outbox-support at the root, so Docker needs to see
# the whole tree to run a module-aware Gradle build:
#
#   docker build -f shipping-service/Dockerfile .
#
# (docker-compose.yml's `build.context: .` / `build.dockerfile:
# shipping-service/Dockerfile` does this automatically.)

# ---- Stage 1: build ----
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace

# Copy the whole monorepo (see ../.dockerignore for what's excluded —
# build/ output, .git/, IDE files, docs, etc.) and run a module-scoped
# Gradle build. --no-daemon is required in a container: a lingering
# Gradle daemon process would otherwise keep the build stage's container
# alive after the build task finishes.
COPY . .
RUN ./gradlew :shipping-service:bootJar --no-daemon

# ---- Stage 2: runtime ----
FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app

# Only the resulting boot JAR is copied — none of the JDK, Gradle wrapper,
# source tree, or other modules' build output ends up in the runtime image.
COPY --from=build /workspace/shipping-service/build/libs/*.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
