# Exact Commands — Build, Run, Test

Prerequisites on the target machine: **JDK 17+**, **Docker + Docker Compose v2**,
network access, a clone of your Kafka fork.

## 0. Apply the source changes to a fresh Kafka fork

```bash
git clone https://github.com/<your-user>/kafka.git
cd kafka
# (optional, to match the spec exactly) git checkout -b enhance-mm2 4.0.0

# Copy the three changed files from this package into the fork (same paths):
cp <pkg>/kafka-fork-changes/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java        connect/mirror/src/main/java/org/apache/kafka/connect/mirror/
cp <pkg>/kafka-fork-changes/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/LogTruncationException.java  connect/mirror/src/main/java/org/apache/kafka/connect/mirror/
cp <pkg>/kafka-fork-changes/connect/mirror/src/test/java/org/apache/kafka/connect/mirror/MirrorSourceTaskTest.java    connect/mirror/src/test/java/org/apache/kafka/connect/mirror/

git add -A
git commit -m "MirrorMaker 2: fail-fast on log truncation and auto-recover on topic reset"
git push origin enhance-mm2
# Then open a PR from enhance-mm2 on GitHub.
```

## 1. Build (compile) the enhanced MirrorMaker 2

```bash
# From the fork root:
./gradlew :connect:mirror:jar
```

## 2. Test (run the unit tests)

```bash
./gradlew :connect:mirror:test --tests "org.apache.kafka.connect.mirror.MirrorSourceTaskTest"
```

## 3. Build the Docker images

```bash
# Enhanced MM2 (build context = fork root; copy the mm2/ folder into the fork first)
cp -r <pkg>/mm2 .
cp mm2/.dockerignore.fork .dockerignore
docker build -f mm2/Dockerfile -t <your-user>/enhanced-mm2:latest .

# Producer
docker build -t <your-user>/commit-log-producer:latest <pkg>/producer

# Push (for the Docker Hub deliverable)
docker push <your-user>/enhanced-mm2:latest
docker push <your-user>/commit-log-producer:latest
```

## 4. Run the environment

```bash
cd <pkg>            # directory containing docker-compose.yml
export DOCKERHUB_USER=<your-user>
export TAG=latest
docker compose up -d primary-kafka dr-kafka mm2
docker logs -f mm2   # watch MirrorMaker 2
```

## 5. Run the challenge scenarios

```bash
./scripts/run_challenge.sh all          # normal + truncation + reset
# or individually:
./scripts/run_challenge.sh normal
./scripts/run_challenge.sh truncation
./scripts/run_challenge.sh reset
```

## 6. Inspect DR contents manually (optional)

```bash
docker exec dr-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic primary.commit-log \
  --from-beginning --timeout-ms 5000
```

## 7. Teardown

```bash
docker compose down -v
```
