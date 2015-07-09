package io.confluent.streaming;

/**
 * An interface to implement an application logic of a stream processing.
 * An instance is created and initialized by the framework for each partition.
 */
public interface KStreamJob {

  /**
   * Returns a set of topics used in this job
   * @return Topics
   */
  Topics topics();

  /**
   * Initializes a stream processing job for a partition. This method is called for each partition.
   * An application constructs a processing logic using KStream API.
   * <p>
   * For example,
   * </p>
   * <pre>
   *   public init(KStreamContext context) {
   *     KStream&lt;Integer, PageView&gt; pageViewStream = context.from("pageView").mapValues(...);
   *     KStream&lt;Integer, AdClick&gt; adClickStream = context.from("adClick").join(pageViewStream, ...).process(...);
   *   }
   * </pre>
   * @param context KStreamContext for this partition
   */
  void init(KStreamContext context);

  /**
   * Closes this partition of the stream processing job.
   * An application can perform its special clean up here.
   */
  void close();

}
