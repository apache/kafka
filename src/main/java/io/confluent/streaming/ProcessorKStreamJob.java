package io.confluent.streaming;

/**
 * Created by guozhang on 7/14/15.
 */
public abstract class ProcessorKStreamJob<K, V> extends KStreamTopology implements Processor<K, V> {

  @SuppressWarnings("unchecked")
  @Override
<<<<<<< HEAD
<<<<<<< HEAD
  public void init(KStreamContext context) {
<<<<<<< HEAD
=======
    this.streamContext = context;
>>>>>>> add an example for using local state store
    context.from().process((Processor) this);
=======
  public void init(KStreamInitializer initializer) {
    initializer.from().process((Processor) this);
>>>>>>> new api model
=======
  public void topology() {
    ((KStream<K, V>)from()).process(this);
>>>>>>> wip
  }

}
