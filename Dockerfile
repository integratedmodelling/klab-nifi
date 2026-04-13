# Stage 1: Build NAR files with Maven
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /build

# Copy Maven wrapper and POM files first for better layer caching
COPY mvnw mvnw.cmd ./
COPY .mvn .mvn
COPY pom.xml ./
COPY nifi-klab-nifi-api/pom.xml nifi-klab-nifi-api/
COPY nifi-klab-nifi-api-nar/pom.xml nifi-klab-nifi-api-nar/
COPY nifi-klab-nifi/pom.xml nifi-klab-nifi/
COPY nifi-klab-nifi-nar/pom.xml nifi-klab-nifi-nar/

# Fix line endings and download dependencies (cached unless POMs change)
RUN sed -i 's/\r$//' mvnw && chmod +x mvnw && ./mvnw dependency:go-offline -B || true

# Copy source code
COPY nifi-klab-nifi-api nifi-klab-nifi-api
COPY nifi-klab-nifi-api-nar nifi-klab-nifi-api-nar
COPY nifi-klab-nifi nifi-klab-nifi
COPY nifi-klab-nifi-nar nifi-klab-nifi-nar

# Build NAR files
RUN ./mvnw clean package -DskipTests -B

# Stage 2: Custom NiFi image with NARs and Python scripts
FROM apache/nifi:2.4.0

# Copy built NAR files into NiFi lib
COPY --from=builder /build/nifi-klab-nifi-api-nar/target/nifi-klab-nifi-api-nar-1.0.0-SNAPSHOT.nar /opt/nifi/nifi-current/lib/
COPY --from=builder /build/nifi-klab-nifi-nar/target/nifi-klab-nifi-nar-1.0.0-SNAPSHOT.nar /opt/nifi/nifi-current/lib/

# Copy Python scripts into NiFi Python extensions (matching docker-compose behavior)
COPY klab-nifi-py/src/klab_nifi_py/ /opt/nifi/nifi-current/python_extensions/

# Create .klab directory for certificates
RUN mkdir -p /home/nifi/.klab

# Copy entrypoint script
COPY docker-entrypoint.sh /opt/nifi/docker-entrypoint.sh

USER root
RUN chmod +x /opt/nifi/docker-entrypoint.sh
USER nifi

# NiFi web configuration - bind to all interfaces so ports are accessible outside the container
ENV NIFI_WEB_HTTPS_HOST=0.0.0.0
ENV NIFI_WEB_HTTPS_PORT=8443

EXPOSE 8443 3306 3307

HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
  CMD curl -fsk https://127.0.0.1:8443/nifi-api/access/config || exit 1

ENTRYPOINT ["/opt/nifi/docker-entrypoint.sh"]
