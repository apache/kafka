package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test tries to test out the EOS robustness on the client side. It features a {@link TransactionSimulationCoordinator}
 * which handles the incoming transactional produce/metadata requests and gives feedback through an underlying client.
 *
 * Each iteration the transaction manager will append one record through accumulator and commit offset at the same time.
 *
 * Features supported:
 * 
 * 1. Random transaction abort
 * 2. Fault injection on response
 * 3. Random message drop
 */
@Category({IntegrationTest.class})
public class TransactionEventSimulationTest {

    private Logger log = LoggerFactory.getLogger(TransactionEventSimulationTest.class);

    private TransactionManager transactionManager;
    private TransactionSimulationCoordinator transactionCoordinator;
    private Sender sender;
    private final LogContext logContext = new LogContext();

    private final MockTime time = new MockTime();
    private final int requestTimeoutMs = 100;

    private ProducerMetadata metadata = new ProducerMetadata(0, Long.MAX_VALUE, 10,
        new LogContext(), new ClusterResourceListeners(), time);
    private MockClient client = new MockClient(time, metadata);

    @Before
    public void setup() {
        transactionManager = new TransactionManager(logContext, "txn-id",
            100, 0L, new ApiVersions(), false);

        transactionCoordinator = new TransactionSimulationCoordinator(client);
    }

    @Test
    public void simulateTxnEvents() throws InterruptedException {
        RecordAccumulator accumulator = new RecordAccumulator(logContext, 100, CompressionType.GZIP,
            0, 0L, 10, new Metrics(), "accumulator", time, new ApiVersions(), transactionManager,
            new BufferPool(1000, 100, new Metrics(), time, "producer-internal-metrics"));

        metadata.add("topic", time.milliseconds());
        metadata.update(metadata.newMetadataRequestAndVersion(time.milliseconds()).requestVersion,
            TestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 2)), true, time.milliseconds());

        sender = new Sender(logContext, client, metadata, accumulator, false, 100, (short) 1,
            Integer.MAX_VALUE, new SenderMetricsRegistry(new Metrics()), time, requestTimeoutMs, 10, transactionManager, new ApiVersions());

        transactionManager.initializeTransactions();
        sender.runOnce();
        resolvePendingRequests();
        final int numTransactions = 100;

        TopicPartition key = new TopicPartition("topic", 0);
        long committedOffsets = 0L;
        Random abortTxn = new Random();
        client.prepareMetadataUpdate(TestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 2)));

        for (int i = 0; i < numTransactions; i++) {
            transactionManager.beginTransaction();
            transactionManager.maybeAddPartitionToTransaction(key);
            accumulator.append(key, 0L, new byte[1], new byte[1], Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
            transactionManager.sendOffsetsToTransaction(Collections.singletonMap(key, new OffsetAndMetadata(committedOffsets)), new ConsumerGroupMetadata("group"));
            if (abortTxn.nextBoolean()) {
                transactionManager.beginCommit();
                committedOffsets += 1;
            } else {
                transactionManager.beginAbort();
            }

            resolvePendingRequests();
        }

        assertTrue(transactionCoordinator.persistentPartitionData().containsKey(key));
        assertTrue(transactionCoordinator.committedOffsets().containsKey(key));
        assertEquals(committedOffsets - 1, (long) transactionCoordinator.committedOffsets().get(key));
    }

    private void resolvePendingRequests() {
        Random dropMessageRandom = new Random();

        while (!client.requests().isEmpty() || transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION) == null || !transactionManager.isReady()) {
            if (dropMessageRandom.nextInt(5) == 0) {
                transactionCoordinator.runOnce(true);
            } else {
                transactionCoordinator.runOnce(false);
            }

            sender.runOnce();
        }
    }
}
