package org.apache.kafka.clients.producer.simulation;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;

import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The idempotent producer is intended to guarantee two properties:
 *
 * 1. No duplicates: a record sent by the producer will appear in the
 *     log no more than once.
 * 2. No reordering: records will be written to the log in the same order
 *     that they are sent by the producer.
 *
 * The reordering guarantee does not mean that all records sent by
 * the producer will be successfully written to the log. Individual
 * records may fail to be written for a number of reasons. For example,
 * if the delivery timeout is reached before a record could be delivered
 * to the partition leader, then  . In other words, the failure of
 * individual records does not prevent subsequent records from being
 * delivered.
 */
public class IdempotentProducerSimulationTest {

    @Property(tries = 10_000)
    public void testTransientNotEnoughReplicasError(
        @ForAll Random random,
        @ForAll @IntRange(min = 1, max = 3) int numPartitions,
        @ForAll @IntRange(min = 1, max = 5) int maxInflights
    ) {
        SimulationContext context = new SimulationContext.Builder(random)
            .setNumPartitions(numPartitions)
            .setPartitioner(new RandomPartitioner(random))
            .addBroker(new Node(0, "localhost", 9092))
            .setProducerConfig(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInflights)
            .setProducerConfig(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100)
            .setProducerConfig(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 10)
            .setProducerConfig(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 500)
            .build();

        context.run(context.random.nextInt(100));
        context.cluster.enableUnderMinIsr(0, 10);
        context.runUntilSendsCompleted();

        try {
            context.cluster.assertNoDuplicates();
            context.cluster.assertNoReordering();
            context.assertNoSendFailures();
        } catch (Throwable t) {
            context.printEventTrace();
            throw t;
        }
    }

    @Property(tries = 10_000)
    public void testFatalInvalidRecordErrors(
        @ForAll Random random,
        @ForAll @IntRange(min = 1, max = 3) int numPartitions,
        @ForAll @IntRange(min = 1, max = 5) int maxInflights
    ) {
        SimulationContext context = new SimulationContext.Builder(random)
            .setNumPartitions(numPartitions)
            .addBroker(new Node(0, "localhost", 9092))
            .setPartitioner(new RandomPartitioner(random))
            .setProducerConfig(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInflights)
            .setProducerConfig(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100)
            .setProducerConfig(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 10)
            .setProducerConfig(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 500)
            .build();

        context.run(context.random.nextInt(100));
        context.cluster.enableInvalidRecordError(0, 20);
        context.run(10);
        context.client.randomDisconnect();
        context.runUntilSendsCompleted();

        try {
            context.cluster.assertNoDuplicates();
            context.cluster.assertNoReordering();
            context.assertSendFailuresOnlyWithError(Errors.INVALID_RECORD);
        } catch (Throwable t) {
            context.printEventTrace();
            throw t;
        }
    }

    @Property(tries = 10_000)
    public void testRandomNetworkDisconnects(
        @ForAll Random random,
        @ForAll @IntRange(min = 1, max = 3) int numPartitions,
        @ForAll @IntRange(min = 1, max = 5) int maxInflights
    ) {
        SimulationContext context = new SimulationContext.Builder(random)
            .setNumPartitions(numPartitions)
            .addBroker(new Node(0, "localhost", 9092))
            .setPartitioner(new RandomPartitioner(random))
            .setProducerConfig(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInflights)
            .setProducerConfig(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100)
            .setProducerConfig(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 10)
            .setProducerConfig(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 500)
            .build();

        context.actions.add(() -> {
            context.client.randomDisconnect();
            return true;
        });
        context.runUntilSendsCompleted();

        try {
            context.cluster.assertNoDuplicates();
            context.cluster.assertNoReordering();

            // With random disconnects, delivery timeouts are possible
            context.assertSendFailuresOnlyWithError(Errors.REQUEST_TIMED_OUT);
        } catch (Throwable t) {
            context.printEventTrace();
            throw t;
        }
    }

    /**
     * In tests involving multiple partitions, it's helpful to ensure writes are
     * interleaved. If an error occurs on only one partition, we want to verify
     * that the writes to other partitions are not impacted (e.g. we want to ensure
     * that epoch bumps do cause duplicates). It is difficult to get this coverage
     * with the default sticky partitioning logic.
     */
    private static class RandomPartitioner implements Partitioner {
        private final Random random;

        private RandomPartitioner(Random random) {
            this.random = random;
        }

        @Override
        public int partition(
            String topic,
            Object key,
            byte[] keyBytes,
            Object value,
            byte[] valueBytes,
            Cluster cluster
        ) {
            Integer numPartitions = cluster.partitionCountForTopic(topic);
            if (numPartitions == null) {
                throw new IllegalStateException("Could not determine partition count for topic" + topic);
            } else {
                return random.nextInt(numPartitions);
            }
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

}
