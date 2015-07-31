package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamJob;
import org.apache.kafka.common.utils.Utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by yasuhiro on 7/30/15.
 */
public class TopologyAnalyzer {

  public final Set<String> topics;
  public final Collection<KStreamSource<?, ?>> streams;

  public TopologyAnalyzer(Class<? extends KStreamJob> jobClass) {
    KStreamJob job = (KStreamJob) Utils.newInstance(jobClass);
    KStreamInitializerImpl context = new KStreamInitializerImpl();

    job.init(context);

    this.streams = context.sourceStreams();
    Set<String> topics = new HashSet<>();
    for (KStreamSource<?, ?> stream : this.streams) {
      for (String topic : stream.topics) {
        topics.add(topic);
      }
    }
    this.topics = Collections.unmodifiableSet(topics);
  }

}
