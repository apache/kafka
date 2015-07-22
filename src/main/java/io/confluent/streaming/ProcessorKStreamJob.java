package io.confluent.streaming;

import org.apache.kafka.common.utils.Utils;

/**
 * Created by guozhang on 7/14/15.
 */
public abstract class ProcessorKStreamJob<K, V> implements KStreamJob, Processor<K, V> {

  @SuppressWarnings("unchecked")
  @Override
  public void init(KStreamContext context) {
    context.from(null).process((Processor) this);
  }

  @Override
  public void close() {
    // do nothing
  }
}
