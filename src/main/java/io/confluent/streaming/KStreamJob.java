package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/22/15.
 */
public interface KStreamJob {

  void configure(KStreamConfig config);

  void build(KStreamContext ksc);

}
