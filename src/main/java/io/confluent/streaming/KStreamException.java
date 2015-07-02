package io.confluent.streaming;

/**
 * Created by yasuhiro on 7/2/15.
 */
public class KStreamException extends RuntimeException {

  public KStreamException(String msg) {
    super(msg);
  }

  public KStreamException(Throwable t) {
    super(t);
  }

  public KStreamException(String msg, Throwable t) {
    super(msg, t);
  }
}
