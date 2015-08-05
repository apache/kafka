package org.apache.kafka.stream.internal;


import org.apache.kafka.stream.topology.Processor;
import org.apache.kafka.stream.util.Stamped;

public class PunctuationSchedule extends Stamped<Processor<?,? >> {

  final long interval;

  public PunctuationSchedule(Processor<?, ?> processor, long interval) {
    super(processor, System.currentTimeMillis() + interval);
    this.interval = interval;
  }

  public Processor<?, ?> processor() {
    return value;
  }

  public PunctuationSchedule next() {
    return new PunctuationSchedule(value, timestamp + interval);
  }

}
