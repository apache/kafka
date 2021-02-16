package org.apache.kafka.streams.header;

import java.util.Iterator;
import org.apache.kafka.common.header.internals.RecordHeaders;

public class StreamHeaders implements Headers {

  public static Headers fromRecordHeaders(org.apache.kafka.common.header.Headers headers) {
    return null;
  }

  @Override
  public RecordHeaders toRecordHeaders() {
    return null;
  }

  @Override
  public Iterator<Header> iterator() {
    return null;
  }
}
