package io.confluent.streaming;

/**
 * KStream is an abstraction of a stream of key-value pairs.
 */
public interface KStream<K, V> {

  /**
   * Creates a new stream consists of all elements of this stream which satisfy a predicate
   * @param predicate
   * @return KStream
   */
  KStream<K, V> filter(Predicate<K, V> predicate);

  /**
   * Creates a new stream consists all elements of this stream which do not satisfy a predicate
   * @param predicate
   * @return
   */
  KStream<K, V> filterOut(Predicate<K, V> predicate);

  /**
   * Creates a new stream by transforming key-value pairs by a mapper to all elements of this stream
   * @param mapper
   * @return KStream
   */
  <K1, V1> KStream<K1, V1> map(KeyValueMapper<K1, V1, K, V> mapper);

  /**
   * Creates a new stream by transforming valuesa by a mapper to all values of this stream
   * @param mapper
   * @return
   */
  <V1> KStream<K, V1> mapValues(ValueMapper<V1, V> mapper);

  /**
   * Creates a new stream by applying a mapper to all elements of this stream and using the values in the resulting Iterable
   * @param mapper
   * @return
   */
  <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K1, ? extends Iterable<V1>, K, V> mapper);

  /**
   * Creates a new stream by applying a mapper to all values of this stream and using the values in the resulting Iterable
   * @param processor
   * @return
   */
  <V1> KStream<K, V1> flatMapValues(ValueMapper<? extends Iterable<V1>, V> processor);

  /**
   * Creates a new windowed stream using a specified window object.
   * @param window
   * @return
   */
  KStreamWindowed<K, V> with(Window<K, V> window);

  /**
   * Creates a new stream by joining this stream with the other windowed stream.
   * Each element in this stream is joined with elements in the other stream's window.
   * The resulting values are computed by applying a joiner.
   *
   * @param other
   * @param joiner
   * @return KStream
   * @throws NotCopartitionedException
   */
  <V1, V2> KStream<K, V2> nestedLoop(KStreamWindowed<K, V1> other, ValueJoiner<V2, V, V1> joiner)
    throws NotCopartitionedException;

  /**
   * Creates an array of streams from this stream. Each stream in the array coresponds to a predicate in
   * supplied predicates in the same order. Predicates are evaluated in order. An element is streamed to
   * a corresponding stream for the first predicate is evaluated true.
   * An element will be dropped if none of the predicates evaluate true.
   * @param predicates
   * @return
   */
  KStream<K, V>[] branch(Predicate<K, V>... predicates);

  /**
   * Sends key-value to a topic, also creates a new stream from the topic.
   * This is equivalent to calling sendTo(topic) and KStreamContext.from(topic).
   * @param topic
   * @return
   */
  KStream<K, V> through(String topic);

  /**
   * Sends key-value to a topic.
   * @param topic
   */
  void sendTo(String topic);

  /**
   * Processes all elements in this stream by applying a processor.
   * @param processor
   */
  void process(Processor<K, V> processor);

}
