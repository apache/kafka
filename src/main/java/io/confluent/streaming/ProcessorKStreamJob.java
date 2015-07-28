package io.confluent.streaming;

/**
 * Created by guozhang on 7/14/15.
 */
public abstract class ProcessorKStreamJob<K, V> implements KStreamJob, Processor<K, V> {

  protected KStreamContext streamContext;

  @SuppressWarnings("unchecked")
  @Override
  public void init(KStreamContext context) {
<<<<<<< HEAD
=======
    this.streamContext = context;
>>>>>>> add an example for using local state store
    context.from().process((Processor) this);
  }

  @Override
  public void close() {
    // do nothing
  }
}
