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

# Download dependencies (cached unless POMs change)
RUN chmod +x mvnw && ./mvnw dependency:go-offline -B || true

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

# Copy Python scripts into NiFi Python extensions
COPY klab-nifi-py/src/klab_nifi_py /opt/nifi/nifi-current/python_extensions/klab_nifi_py
