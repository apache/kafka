package io.confluent.streaming;

/**
 * An interface that allows the KStream framework to extract a timestamp from a key-value pair
 */
public interface TimestampExtractor {

  /**
   * Extracts a timestamp from a key-value pair from a topic
   * @param topic the topic name
   * @param key the key object
   * @param value the value object
   * @return timestamp
   */
  long extract(String topic, Object key, Object value);

}
