package org.apache.kafka.stream.topology;

import org.apache.kafka.clients.processor.Processor;
import org.apache.kafka.common.utils.Utils;

/**
 * Created by guozhang on 7/14/15.
 */
public class SingleProcessorTopology extends KStreamTopology {

  private final Class<? extends Processor> processorClass;
  private final String[] topics;

  public SingleProcessorTopology(Class<? extends Processor> processorClass, String... topics) {
    this.processorClass = processorClass;
    this.topics = topics;
  }
  @SuppressWarnings("unchecked")
  @Override
  public void topology() {
    from(topics).process(newProcessor());
  }

  @SuppressWarnings("unchecked")
  private Processor newProcessor() {
    return (Processor) Utils.newInstance(processorClass);
  }
}
