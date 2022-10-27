package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducerTest;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.SenderMetricsRegistry;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SimulationContext {
    final LogContext logContext;
    final Random random;
    final Metrics metrics;
    final MockTime time;
    final MockKafkaClient client;
    final List<SimulationAction> actions;
    final ClusterModel cluster;
    final Partitioner partitioner;

    final ProducerConfig config;
    final ProducerMetadata metadata;
    final int maxRecordsToSend;

    private int numPolls = 0;
    private int numSendsCompleted = 0;
    private int nextRecordId = 0;


    private SimulationContext(
        Random random,
        List<Node> brokers,
        int numPartitions,
        ProducerConfig config,
        int maxRecordsToSend,
        boolean enableStrictValidation,
        Partitioner partitioner
    ) {
        this.config = config;
        this.logContext = new LogContext();
        this.random = random;
        this.maxRecordsToSend = maxRecordsToSend;
        this.partitioner = partitioner;
        this.time = new MockTime();
        this.metrics = new Metrics();
        this.actions = new ArrayList<>();
        this.metadata = new ProducerMetadata(
            config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
            config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
            config.getLong(ProducerConfig.METADATA_MAX_IDLE_CONFIG),
            logContext,
            new ClusterResourceListeners(),
            time
        );
        this.cluster = new ClusterModel(
            time,
            random,
            brokers,
            numPartitions,
            enableStrictValidation
        );
        this.client = new MockKafkaClient(
            cluster,
            brokers,
            metadata,
            time,
            random,
            config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
            config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)
        );

        addProducerActions();
        addClusterActions();
    }

    boolean allSendsCompleted() {
        return nextRecordId == maxRecordsToSend;
    }

    void addClusterActions() {
        actions.add(() -> {
            cluster.poll();
            return true;
        });
    }

    void runRandomAction() {
        int randomIdx = random.nextInt(actions.size());
        SimulationAction action = actions.get(randomIdx);
        if (action.maybeRun()) {
            time.sleep(1);
            numPolls++;
        }
    }

    void runUntilSendsCompleted() {
        runUntil(this::allSendsCompleted);
    }

    void run(int depth) {
        int currentPolls = numPolls;
        runWhile(() -> numPolls - currentPolls < depth);
    }

    void runUntil(Supplier<Boolean> breakCondition) {
        while (!breakCondition.get()) {
            runRandomAction();
        }
    }

    void runWhile(Supplier<Boolean> loopCondition) {
        while (loopCondition.get()) {
            runRandomAction();
        }
    }

    void assertNoSendFailures() {
        List<SimulationEvent> failedSends = cluster.events.stream()
            .filter(event -> event instanceof FailedSend)
            .collect(Collectors.toList());
        assertEquals(Collections.emptyList(), failedSends);
    }

    void assertSendFailuresOnlyWithError(Errors error) {
        List<SimulationEvent> failedSends = cluster.events.stream()
            .filter(event -> {
                if (event instanceof FailedSend) {
                    FailedSend failedSend = (FailedSend) event;
                    return Errors.forException(failedSend.exception) != error;
                } else {
                    return false;
                }
            })
            .collect(Collectors.toList());
        assertEquals(Collections.emptyList(), failedSends);
    }

    void printEventTrace(int maxEvents) {
        cluster.events.stream()
            .limit(maxEvents)
            .forEach(System.out::println);
    }

    void printEventTrace() {
        printEventTrace(Integer.MAX_VALUE);
    }

    void addProducerActions() {
        ApiVersions apiVersions = new ApiVersions();
        long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);

        TransactionManager txnManager = new TransactionManager(
            logContext,
            config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG),
            config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG),
            retryBackoffMs,
            apiVersions
        );

        BufferPool bufferPool = new BufferPool(
            config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG),
            config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
            metrics,
            time,
            KafkaProducer.PRODUCER_METRIC_GROUP_NAME
        );

        RecordAccumulator.PartitionerConfig partitionerConfig = new RecordAccumulator.PartitionerConfig(
            this.partitioner == null,
            config.getLong(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG)
        );

        RecordAccumulator accumulator = new RecordAccumulator(
            logContext,
            config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
            CompressionType.NONE,
            Math.toIntExact(config.getLong(ProducerConfig.LINGER_MS_CONFIG)),
            retryBackoffMs,
            config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG),
            partitionerConfig,
            metrics,
            KafkaProducer.PRODUCER_METRIC_GROUP_NAME,
            time,
            apiVersions,
            txnManager,
            bufferPool
        );

        // Do an initial metadata update in order to prevent `send` from blocking.
        metadata.add(cluster.topic, time.milliseconds());
        int requestVersion = metadata.requestUpdateForTopic(cluster.topic);
        metadata.update(requestVersion, cluster.handleMetadataRequest(
            metadata.newMetadataRequestBuilder().build()
        ), false, time.milliseconds());

        Sender sender = new Sender(
            logContext,
            client,
            metadata,
            accumulator,
            false,
            config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
            Short.parseShort(config.getString(ProducerConfig.ACKS_CONFIG)),
            config.getInt(ProducerConfig.RETRIES_CONFIG),
            new SenderMetricsRegistry(metrics),
            time,
            config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG),
            retryBackoffMs,
            txnManager,
            apiVersions
        );

        actions.add(() -> {
            sender.runOnce();
            return true;
        });

        KafkaProducer<Long, Long> producer = KafkaProducerTest.newKafkaProducer(
            config,
            logContext,
            metrics,
            new LongSerializer(),
            new LongSerializer(),
            metadata,
            accumulator,
            txnManager,
            sender,
            partitioner,
            time,
            Mockito.mock(KafkaThread.class)
        );

        actions.add(() -> {
            if (allSendsCompleted()) {
                return false;
            }

            Long value = (long) nextRecordId++;
            ProducerRecord<Long, Long> record = new ProducerRecord<>(cluster.topic, value);
            cluster.events.add(new RecordSent(record));

            producer.send(record, (recordMetadata, exception) -> {
                numSendsCompleted++;
                if (exception != null) {
                    cluster.events.add(new FailedSend(record, exception));
                } else {
                    cluster.events.add(new SuccessfulSend(record));
                }
            });

            return true;
        });
    }

    public static class Builder {
        private final Random random;
        private final Properties properties;
        private int maxRecordsToSend = 100;
        private int numPartitions = 1;
        private boolean enableStrictValidation = true;
        private List<Node> brokers = new ArrayList<>();

        private Partitioner partitioner;

        public Builder(Random random) {
            this.random = random;
            this.properties = new Properties();
            this.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            this.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        }

        Builder setProducerConfig(String config, Object value) {
            this.properties.put(config, value);
            return this;
        }

        Builder addBroker(Node node) {
            this.brokers.add(node);
            return this;
        }

        Builder setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        Builder disableStrictValidation() {
            this.enableStrictValidation = false;
            return this;
        }

        Builder setPartitioner(Partitioner partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        SimulationContext build() {
            return new SimulationContext(
                random,
                brokers,
                numPartitions,
                new ProducerConfig(properties),
                maxRecordsToSend,
                enableStrictValidation,
                partitioner
            );
        }
    }
}
