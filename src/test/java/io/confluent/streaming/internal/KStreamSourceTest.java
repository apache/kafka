package io.confluent.streaming.internal;

import static org.junit.Assert.assertEquals;

import io.confluent.streaming.*;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;

public class KStreamSourceTest {

  @Test
  public void testKStreamSource() {

    Ingestor ingestor = new Ingestor() {

      @Override
      public void poll() {}

      @Override
      public void poll(long timeoutMs) {}

      @Override
      public void pause(TopicPartition partition) {}

      @Override
      public void unpause(TopicPartition partition, long offset) {}
    };

    StreamSynchronizer<String, String> streamSynchronizer = new StreamSynchronizer<String, String>(
      "group",
      ingestor,
      new ChooserImpl<String, String>(),
      new TimestampExtractor<String, String>() {
        public long extract(String topic, String key, String value) {
          return 0L;
        }
      },
      10
    );

    PartitioningInfo partitioningInfo = new PartitioningInfo(new SyncGroup("group", streamSynchronizer), 1);

    final ArrayList<String> processed = new ArrayList<String>();

    Processor<String, String> processor = new Processor<String, String>() {

      @Override
      public void apply(String key, String value) {
        processed.add(key + ":" + value);
      }

      @Override
      public void init(PunctuationScheduler punctuationScheduler) {}

      @Override
      public void punctuate(long streamTime) {}
    };

    KStreamSource<String, String> stream = new KStreamSource<String, String>(partitioningInfo, null);
    stream.process(processor);

    final String[] expectedKeys = new String[] { "k1", "k2", "k3" };
    final String[] expectedValues = new String[] { "v1", "v2", "v3" };

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], expectedValues[i], 0L);
    }

    assertEquals(3, processed.size());

    for (int i = 0; i < expectedKeys.length; i++) {
      assertEquals(expectedKeys[i] + ":" + expectedValues[i], processed.get(i));
    }
  }

}
