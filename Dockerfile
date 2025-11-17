# Multi-stage build for optimal image size
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app

# Copy pom.xml and download dependencies (cached layer)
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build
COPY src ./src
RUN mvn clean package -DskipTests

# Runtime image
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Create non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy JAR from builder stage
COPY --from=builder /app/target/candlestick-processor-*.jar app.jar

# Create state directory
RUN mkdir -p /var/kafka-streams && chown -R appuser:appgroup /var/kafka-streams

USER appuser

# Expose metrics port (if using JMX)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD pgrep -f "candlestick-processor" > /dev/null || exit 1

# Run the application
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]