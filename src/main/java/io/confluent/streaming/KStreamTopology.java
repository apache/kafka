package io.confluent.streaming;

import io.confluent.streaming.internal.KStreamSource;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * KStreamTopology is the class that allows an implementation of {@link KStreamTopology#topology()} to create KStream instances.
 *
 */
public abstract class KStreamTopology {

  private final ArrayList<KStreamSource<?, ?>> streams = new ArrayList<>();

  /**
   * Initializes a stream processing topology. This method may be called multiple times.
   * An application constructs a processing logic using KStream API.
   * <p>
   * For example,
   * </p>
   * <pre>
   *   KStreamTopology topology = new KStreamTopology() {
   *     public void topology() {
   *       KStream&lt;Integer, PageView&gt; pageViewStream = from("pageView").mapValues(...);
   *       KStream&lt;Integer, AdClick&gt; adClickStream = from("adClick").join(pageViewStream, ...).process(...);
   *     }
   *   }
   *
   *   KafkaStreaming streaming = new KafkaStreaming(topology, streamingConfig)
   *   streaming.run();
   * </pre>
   */
  public abstract void topology();

  /**
   * Extracts topics used in the KStream topology. This method calls {@link KStreamTopology#topology()} method.
   * @return
   */
  public final Set<String> topics() {
    synchronized (streams) {
      try {
        streams.clear();
        topology();
        Set<String> topics = new HashSet<>();
        for (KStreamSource<?, ?> stream : streams) {
          topics.addAll(stream.topics());
        }
        return topics;
      }
      finally {
        streams.clear();
      }
    }
  }

  /**
   * Returns source streams in the KStream topology. This method calls {@link KStreamTopology#topology()} method.
   * This method may be called multiple times.
   */
  public final Collection<KStreamSource<?, ?>> sourceStreams() {
    synchronized (streams) {
      try {
        streams.clear();
        topology();
        return new ArrayList<>(streams);
      }
      finally {
        streams.clear();
      }
    }
  }


  // TODO: support regex topic matching in from() calls, for example:
  // context.from("Topic*PageView")

  /**
   * Creates a KStream instance for the specified topics. The stream is added to the default synchronization group.
   * @param topics the topic names, if empty default to all the topics in the config
   * @return KStream
   */
  public KStream<?, ?> from(String... topics) {
    return from(null, null, topics);
  }

  /**
   * Creates a KStream instance for the specified topic. The stream is added to the default synchronization group.
   * @param keyDeserializer key deserializer used to read this source KStream,
   *                        if not specified the default deserializer defined in the configs will be used
   * @param valDeserializer value deserializer used to read this source KStream,
   *                        if not specified the default deserializer defined in the configs will be used
   * @param topics the topic names, if empty default to all the topics in the config
   * @return KStream
   */
  public <K, V> KStream<K, V> from(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {
    KStreamSource<K, V> stream = new KStreamSource<>(topics, keyDeserializer, valDeserializer, this);
    streams.add(stream);
    return stream;
  }

}
