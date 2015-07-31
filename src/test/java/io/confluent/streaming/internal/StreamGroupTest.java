package io.confluent.streaming.internal;

import io.confluent.streaming.TimestampExtractor;
import io.confluent.streaming.testutil.MockIngestor;
import io.confluent.streaming.testutil.MockKStreamContext;
import io.confluent.streaming.testutil.MockKStreamTopology;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamGroupTest {

  private static Serializer<Integer> serializer = new IntegerSerializer();
  private static Deserializer<Integer> deserializer = new IntegerDeserializer();

  private static class MockKStreamSource extends KStreamSource<Integer, Integer> {

    public int numReceived = 0;
    public ArrayList<Object> keys = new ArrayList<>();
    public ArrayList<Object> values = new ArrayList<>();
    public ArrayList<Long> timestamps = new ArrayList<>();

    public MockKStreamSource() {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
      super(null, deserializer, deserializer, new MockKStreamTopology());
=======
      super(null, deserializer, deserializer, new KStreamInitializerImpl(serializer, serializer, deserializer, deserializer));
>>>>>>> new api model
=======
      super(null, deserializer, deserializer, new KStreamInitializerImpl());
>>>>>>> wip
=======
      super(null, deserializer, deserializer, new MockKStreamTopology());
>>>>>>> wip
    }

    @Override
    public void receive(Object key, Object value, long timestamp) {
      this.numReceived++;
      this.keys.add(key);
      this.values.add(value);
      this.timestamps.add(timestamp);
    }

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAddPartition() {

    MockIngestor mockIngestor = new MockIngestor();

    StreamGroup streamGroup = new StreamGroup(
      new MockKStreamContext(serializer, deserializer),
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
    MockKStreamSource stream1 = new MockKStreamSource();
    MockKStreamSource stream2 = new MockKStreamSource();
    MockKStreamSource stream3 = new MockKStreamSource();

    streamGroup.addPartition(partition1, stream1);
    mockIngestor.addPartitionStreamToGroup(streamGroup, partition1);

    streamGroup.addPartition(partition2, stream2);
    mockIngestor.addPartitionStreamToGroup(streamGroup, partition2);

    Exception exception = null;
    try {
      streamGroup.addPartition(partition1, stream3);
    } catch (Exception ex) {
      exception = ex;
    }
    assertTrue(exception != null);

    byte[] recordValue = serializer.serialize(null, new Integer(10));

    mockIngestor.addRecords(partition1, records(
      new ConsumerRecord<>(partition1.topic(), partition1.partition(), 1, serializer.serialize(partition1.topic(), new Integer(10)), recordValue),
      new ConsumerRecord<>(partition1.topic(), partition1.partition(), 2, serializer.serialize(partition1.topic(), new Integer(20)), recordValue)
    ));

    mockIngestor.addRecords(partition2, records(
      new ConsumerRecord<>(partition2.topic(), partition2.partition(), 1, serializer.serialize(partition1.topic(), new Integer(300)), recordValue),
      new ConsumerRecord<>(partition2.topic(), partition2.partition(), 2, serializer.serialize(partition1.topic(), new Integer(400)), recordValue),
      new ConsumerRecord<>(partition2.topic(), partition2.partition(), 3, serializer.serialize(partition1.topic(), new Integer(500)), recordValue),
      new ConsumerRecord<>(partition2.topic(), partition2.partition(), 4, serializer.serialize(partition1.topic(), new Integer(600)), recordValue)
    ));

    streamGroup.process();
    assertEquals(stream1.numReceived, 1);
    assertEquals(stream2.numReceived, 0);

    assertEquals(mockIngestor.paused.size(), 1);
    assertTrue(mockIngestor.paused.contains(partition2));

    mockIngestor.addRecords(partition1, records(
      new ConsumerRecord<>(partition1.topic(), partition1.partition(), 3, serializer.serialize(partition1.topic(), new Integer(30)), recordValue),
      new ConsumerRecord<>(partition1.topic(), partition1.partition(), 4, serializer.serialize(partition1.topic(), new Integer(40)), recordValue),
      new ConsumerRecord<>(partition1.topic(), partition1.partition(), 5, serializer.serialize(partition1.topic(), new Integer(50)), recordValue)
    ));

    streamGroup.process();
    assertEquals(stream1.numReceived, 2);
    assertEquals(stream2.numReceived, 0);

    assertEquals(mockIngestor.paused.size(), 2);
    assertTrue(mockIngestor.paused.contains(partition1));
    assertTrue(mockIngestor.paused.contains(partition2));

    streamGroup.process();
    assertEquals(stream1.numReceived, 3);
    assertEquals(stream2.numReceived, 0);

    streamGroup.process();
    assertEquals(stream1.numReceived, 3);
    assertEquals(stream2.numReceived, 1);

    assertEquals(mockIngestor.paused.size(), 1);
    assertTrue(mockIngestor.paused.contains(partition2));

    streamGroup.process();
    assertEquals(stream1.numReceived, 4);
    assertEquals(stream2.numReceived, 1);

    assertEquals(mockIngestor.paused.size(), 1);

    streamGroup.process();
    assertEquals(stream1.numReceived, 4);
    assertEquals(stream2.numReceived, 2);

    assertEquals(mockIngestor.paused.size(), 0);

    streamGroup.process();
    assertEquals(stream1.numReceived, 5);
    assertEquals(stream2.numReceived, 2);

    streamGroup.process();
    assertEquals(stream1.numReceived, 5);
    assertEquals(stream2.numReceived, 3);

    streamGroup.process();
    assertEquals(stream1.numReceived, 5);
    assertEquals(stream2.numReceived, 4);

    assertEquals(mockIngestor.paused.size(), 0);

    streamGroup.process();
    assertEquals(stream1.numReceived, 5);
    assertEquals(stream2.numReceived, 4);
  }

  private Iterable<ConsumerRecord<byte[], byte[]>> records(ConsumerRecord<byte[], byte[]>... recs) {
    return Arrays.asList(recs);
  }
}
