package org.apache.kafka.tools;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.tools.TransactionalMessageCopier.TransactionalMessageCopierOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TransactionalMessageCopierTest {

  @Before
  public void setUp() {
    Exit.setExitProcedure((statusCode, message) -> {
      throw new IllegalArgumentException(message);
    });
  }

  @After
  public void tearDown() {
    Exit.resetExitProcedure();
  }

  @Test
  public void testOptionsWorkCorrectly() {
    String[] args = new String[]{
        "--bootstrap-server", "localhost:9091,localhost:9092",
        "--input-partition", "10",
        "--output-topic", "hello",
        "--max-messages", "200",
        "--consumer-group", "group",
        "--transaction-size", "2",
        "--transaction-timeout", "1",
        "--transactional-id", "tid",
        "--enable-random-aborts",
        "--group-mode",
        "--use-group-metadata"
    };

    TransactionalMessageCopierOptions opts = TransactionalMessageCopierOptions.parse(args);
    assertEquals("localhost:9091,localhost:9092", opts.brokerList);
    assertEquals(Integer.valueOf(10), opts.inputPartition);
    assertEquals("hello", opts.outputTopic);
    assertEquals(Long.valueOf(200), opts.maxMessages);
    assertEquals("group", opts.consumerGroup);
    assertEquals(Integer.valueOf(2), opts.messagesPerTransaction);
    assertEquals(Integer.valueOf(1), opts.transactionTimeout);
    assertEquals("tid", opts.transactionalId);
    assertTrue(opts.enableRandomAborts);
    assertTrue(opts.groupMode);
    assertTrue(opts.useGroupMetadata);
  }

  @Test
  public void testOptionsDefaults() {
    String[] args = new String[]{
        "--bootstrap-server", "localhost:9091,localhost:9092",
        "--input-partition", "10",
        "--output-topic", "hello",
        "--transactional-id", "tid",
    };
    TransactionalMessageCopierOptions opts = TransactionalMessageCopierOptions.parse(args);
    assertEquals("localhost:9091,localhost:9092", opts.brokerList);
    assertEquals(Integer.valueOf(10), opts.inputPartition);
    assertEquals("hello", opts.outputTopic);
    assertEquals("tid", opts.transactionalId);

    // defaults
    assertEquals(Long.valueOf(Long.MAX_VALUE), opts.maxMessages);
    assertEquals("-1", opts.consumerGroup);
    assertEquals(Integer.valueOf(200), opts.messagesPerTransaction);
    assertEquals(Integer.valueOf(60000), opts.transactionTimeout);
    assertFalse(opts.enableRandomAborts);
    assertFalse(opts.groupMode);
    assertFalse(opts.useGroupMetadata);
  }

  @Test
  public void testBrokerListArg() {
    String[] args = new String[]{
        "--broker-list", "localhost:9091",
        "--input-partition", "10",
        "--output-topic", "hello",
        "--max-messages", "200",
        "--consumer-group", "group",
        "--transaction-size", "2",
        "--transaction-timeout", "1",
        "--transactional-id", "tid",
        "--enable-random-aborts",
        "--group-mode",
        "--use-group-metadata"
    };

    TransactionalMessageCopierOptions opts = TransactionalMessageCopierOptions.parse(args);
    assertEquals("localhost:9091", opts.brokerList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBrokerListArgAndBootstrapServerAreMutuallyExclusive() {
    String[] args = new String[]{
        "--broker-list", "localhost:9091",
        "--bootstrap-server", "localhost:9091,localhost:9092",
        "--input-partition", "10",
        "--output-topic", "hello",
        "--max-messages", "200",
        "--consumer-group", "group",
        "--transaction-size", "2",
        "--transaction-timeout", "1",
        "--transactional-id", "tid",
        "--enable-random-aborts",
        "--group-mode",
        "--use-group-metadata"
    };
    TransactionalMessageCopierOptions.parse(args);
  }
}
