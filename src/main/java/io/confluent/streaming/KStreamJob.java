package io.confluent.streaming;

import io.confluent.streaming.util.Util;

import java.util.Collections;
import java.util.Set;

/**
 * An interface to implement an application logic of a stream processing.
 *
 * Each subclass must declare a public static final member variable <code>topics</code> like this:
 * <pre>
 *   class MyKStreamJob {
 *     public static final Topics topics = new Topics("pageView", "adClick");
 *     ...
 *   }
 * </pre>
 *
 * <code>Topics</code> represents a set of topics used in the application.
 */
public abstract class KStreamJob {

  /**
   * A class represents a set of topic names.
   */
  public static class Topics {

    public final Set<String> set;

    Topics(String...  topics) {
      set = Collections.unmodifiableSet(Util.mkSet(topics));
    }

  }

  /**
   * Initializes a streaming job by constructing a processing logic using KStream API.
   * <p>
   * For exmaple,
   * </p>
   * <pre>
   *   public init(KStreamContext context) {
   *     KStream&ltInteger, PageView> pageViewStream = context.from("pageView").mapValues(...)
   *     KStream&ltInteger, AdClick> adClickStream = context.from("adClick").join(pageViewStream, ...).process(...);
   *   }
   * </pre>
   * @param context
   */
  public abstract void init(KStreamContext context);

}
