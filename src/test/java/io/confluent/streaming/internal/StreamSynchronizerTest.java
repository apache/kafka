package io.confluent.streaming.internal;

import io.confluent.streaming.TimestampExtractor;
import io.confluent.streaming.testutil.MockIngestor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSynchronizerTest {

  private static class MockReceiver implements Receiver {

    public int numReceived = 0;
    public ArrayList<Object> keys = new ArrayList<>();
    public ArrayList<Object> values = new ArrayList<>();
    public ArrayList<Long> timestamps = new ArrayList<>();
    public ArrayList<Long> streamTimes = new ArrayList<>();

    @Override
    public void receive(Object key, Object value, long timestamp, long streamTime) {
      this.numReceived++;
      this.keys.add(key);
      this.values.add(value);
      this.timestamps.add(timestamp);
      this.streamTimes.add(streamTime);
    }

  }

  @Test
  public void testAddPartition() {

    MockIngestor mockIngestor = new MockIngestor();

    StreamSynchronizer streamSynchronizer = new StreamSynchronizer(
      "group",
      mockIngestor,
      new TimeBasedChooser(),
      new TimestampExtractor() {
        public long extract(String topic, Object key, Object value) {
          if (topic.equals("topic1"))
            return ((Integer)key).longValue();
          else
            return ((Integer)key).longValue() / 10L + 5L;
        }
      },
      3
    );

    TopicPartition partition1 = new TopicPartition("topic1", 1);
    TopicPartition partition2 = new TopicPartition("topic2", 1);
    MockReceiver receiver1 = new MockReceiver();
    MockReceiver receiver2 = new MockReceiver();
    MockReceiver receiver3 = new MockReceiver();

    streamSynchronizer.addPartition(partition1, receiver1);
    mockIngestor.addStreamSynchronizerForPartition(streamSynchronizer, partition1);

    streamSynchronizer.addPartition(partition2, receiver2);
    mockIngestor.addStreamSynchronizerForPartition(streamSynchronizer, partition2);

    Exception exception = null;
    try {
      streamSynchronizer.addPartition(partition1, receiver3);
    } catch (Exception ex) {
      exception = ex;
    }
    assertTrue(exception != null);

    mockIngestor.addRecords(partition1, records(
      new ConsumerRecord<Object, Object>(partition1.topic(), partition1.partition(), 1, new Integer(10), "A"),
      new ConsumerRecord<Object, Object>(partition1.topic(), partition1.partition(), 2, new Integer(20), "AA")
    ));

    mockIngestor.addRecords(partition2, records(
      new ConsumerRecord<Object, Object>(partition2.topic(), partition2.partition(), 1, new Integer(300), "B"),
      new ConsumerRecord<Object, Object>(partition2.topic(), partition2.partition(), 2, new Integer(400), "BB"),
      new ConsumerRecord<Object, Object>(partition2.topic(), partition2.partition(), 3, new Integer(500), "BBB"),
      new ConsumerRecord<Object, Object>(partition2.topic(), partition2.partition(), 4, new Integer(600), "BBBB")
    ));

    StreamSynchronizer.Status status = new StreamSynchronizer.Status();

    streamSynchronizer.process(status);
    assertEquals(receiver1.numReceived, 1);
    assertEquals(receiver2.numReceived, 0);

    assertTrue(status.pollRequired());

    assertEquals(mockIngestor.paused.size(), 1);
    assertTrue(mockIngestor.paused.contains(partition2));

    mockIngestor.addRecords(partition1, records(
      new ConsumerRecord<Object, Object>(partition1.topic(), partition1.partition(), 3, new Integer(30), "AAA"),
      new ConsumerRecord<Object, Object>(partition1.topic(), partition1.partition(), 4, new Integer(40), "AAAA"),
      new ConsumerRecord<Object, Object>(partition1.topic(), partition1.partition(), 5, new Integer(50), "AAAAA")
    ));

    streamSynchronizer.process(status);
    assertEquals(receiver1.numReceived, 2);
    assertEquals(receiver2.numReceived, 0);

    assertEquals(mockIngestor.paused.size(), 2);
    assertTrue(mockIngestor.paused.contains(partition1));
    assertTrue(mockIngestor.paused.contains(partition2));

    streamSynchronizer.process(status);
    assertEquals(receiver1.numReceived, 3);
    assertEquals(receiver2.numReceived, 0);

    streamSynchronizer.process(status);
    assertEquals(receiver1.numReceived, 3);
    assertEquals(receiver2.numReceived, 1);

    assertEquals(mockIngestor.paused.size(), 1);
    assertTrue(mockIngestor.paused.contains(partition2));

    streamSynchronizer.process(status);
    assertEquals(receiver1.numReceived, 4);
    assertEquals(receiver2.numReceived, 1);

    assertEquals(mockIngestor.paused.size(), 1);

    streamSynchronizer.process(status);
    assertEquals(receiver1.numReceived, 4);
    assertEquals(receiver2.numReceived, 2);

    assertEquals(mockIngestor.paused.size(), 0);

    streamSynchronizer.process(status);
    assertEquals(receiver1.numReceived, 5);
    assertEquals(receiver2.numReceived, 2);

    streamSynchronizer.process(status);
    assertEquals(receiver1.numReceived, 5);
    assertEquals(receiver2.numReceived, 3);

    streamSynchronizer.process(status);
    assertEquals(receiver1.numReceived, 5);
    assertEquals(receiver2.numReceived, 4);

    assertEquals(mockIngestor.paused.size(), 0);

    streamSynchronizer.process(status);
    assertEquals(receiver1.numReceived, 5);
    assertEquals(receiver2.numReceived, 4);
  }

  private List<ConsumerRecord<Object, Object>> records(ConsumerRecord<Object, Object>... recs) {
    return Arrays.asList(recs);
  }
}
