# Phase 9: query-service container image.
#
# Build context MUST be the repo root, not this directory — this is a
# Gradle multi-module monorepo and query-service depends on :event-schemas
# at the root (unlike the 4 Saga participant services, query-service does
# NOT depend on :outbox-support — it's a pure CQRS read-model consumer,
# never a producer), so Docker needs to see the whole tree to run a
# module-aware Gradle build:
#
#   docker build -f query-service/Dockerfile .
#
# (docker-compose.yml's `build.context: .` / `build.dockerfile:
# query-service/Dockerfile` does this automatically.)

# ---- Stage 1: build ----
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace

# Copy the whole monorepo (see ../.dockerignore for what's excluded —
# build/ output, .git/, IDE files, docs, etc.) and run a module-scoped
# Gradle build. --no-daemon is required in a container: a lingering
# Gradle daemon process would otherwise keep the build stage's container
# alive after the build task finishes.
COPY . .
RUN ./gradlew :query-service:bootJar --no-daemon

# ---- Stage 2: runtime ----
FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app

# Only the resulting boot JAR is copied — none of the JDK, Gradle wrapper,
# source tree, or other modules' build output ends up in the runtime image.
COPY --from=build /workspace/query-service/build/libs/*.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
