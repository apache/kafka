FROM openjdk:11-jdk-slim

# Install necessary tools
RUN apt-get update && apt-get install -y \
    wget \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Set up working directory
WORKDIR /opt/kafka

# Copy the entire Kafka source code
COPY . .

# Build Kafka
RUN ./gradlew jar -PscalaVersion=2.13

# Create kafka bin directory structure
RUN mkdir -p /opt/kafka/bin /opt/kafka/config /opt/kafka/libs

# Copy built JARs to libs
RUN find . -name "*.jar" -path "*/build/libs/*" -exec cp {} /opt/kafka/libs/ \;

# Copy startup scripts
RUN cp bin/* /opt/kafka/bin/ || true

# Copy default configurations
RUN cp config/* /opt/kafka/config/ || true

# Make scripts executable
RUN chmod +x /opt/kafka/bin/*.sh || true

# Set environment variables
ENV KAFKA_HOME=/opt/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin

# Expose ports
EXPOSE 9092

# Default command
CMD ["bash"]