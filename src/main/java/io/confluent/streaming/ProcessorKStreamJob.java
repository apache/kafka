package io.confluent.streaming;

import org.apache.kafka.common.utils.Utils;

/**
 * Created by guozhang on 7/14/15.
 */
public class ProcessorKStreamJob implements KStreamJob {

  public static String PROCESSOR_CLASSNAME = "__PROCESSOR_CLASSNAME__";

  @SuppressWarnings("unchecked")
  @Override
  public void init(KStreamContext context) {
    Processor processor = (Processor) Utils.newInstance((Class<? extends Processor>) context.getContext().get(PROCESSOR_CLASSNAME));

    context.from(null).process(processor);
  }

  @Override
  public void close() {
    // do nothing
  }
}
