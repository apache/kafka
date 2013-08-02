/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.bridge.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class KafkaRecordWriter<K,V> extends RecordWriter<K,V>
{
  protected Producer<Object, byte[]> producer;
  protected String topic;

  protected List<KeyedMessage<Object, byte[]>> msgList = new LinkedList<KeyedMessage<Object, byte[]>>();
  protected int totalBytes = 0;
  protected int queueBytes;

  public KafkaRecordWriter(Producer<Object, byte[]> producer, String topic, int queueBytes)
  {
    this.producer = producer;
    this.topic = topic;
    this.queueBytes = queueBytes;
  }

  protected void sendMsgList() throws IOException
  {
    if (msgList.size() > 0) {
      try {
        producer.send(msgList);
      }
      catch (Exception e) {
        throw new IOException(e);           // all Kafka exceptions become IOExceptions
      }
      msgList.clear();
      totalBytes = 0;
    }
  }

  @Override
  public void write(K key, V value) throws IOException, InterruptedException
  {
    byte[] valBytes;
    if (value instanceof byte[])
      valBytes = (byte[]) value;
    else if (value instanceof BytesWritable)
      // BytesWritable.getBytes returns its internal buffer, so .length would refer to its capacity, not the
      // intended size of the byte array contained.  We need to use BytesWritable.getLength for the true size.
      valBytes = Arrays.copyOf(((BytesWritable) value).getBytes(), ((BytesWritable) value).getLength());
    else
      throw new IllegalArgumentException("KafkaRecordWriter expects byte array value to publish");

    // MultiProducerRequest only supports sending up to Short.MAX_VALUE messages in one batch
    // If the new message is going to make the message list tip over 1 million bytes, send the
    // message list now.
    if ((totalBytes + valBytes.length) > queueBytes || msgList.size() >= Short.MAX_VALUE)
      sendMsgList();

    msgList.add(new KeyedMessage<Object, byte[]>(this.topic, key, valBytes));
    totalBytes += valBytes.length;
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
  {
    sendMsgList();
    producer.close();
  }
}
