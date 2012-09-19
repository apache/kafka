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
import java.util.LinkedList;
import java.util.List;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class KafkaRecordWriter<W extends BytesWritable> extends RecordWriter<NullWritable, W>
{
  protected Producer<Integer, Message> producer;
  protected String topic;

  protected List<ProducerData<Integer, Message>> msgList = new LinkedList<ProducerData<Integer, Message>>();
  protected int totalSize = 0;
  protected int queueSize;

  public KafkaRecordWriter(Producer<Integer, Message> producer, String topic, int queueSize)
  {
    this.producer = producer;
    this.topic = topic;
    this.queueSize = queueSize;
  }

  protected void sendMsgList()
  {
    if (msgList.size() > 0) {
      producer.send(msgList);
      msgList.clear();
      totalSize = 0;
    }
  }

  @Override
  public void write(NullWritable key, BytesWritable value) throws IOException, InterruptedException
  {
    Message msg = new Message(value.getBytes());
    msgList.add(new ProducerData<Integer, Message>(this.topic, msg));
    totalSize += msg.size();

    // MultiProducerRequest only supports sending up to Short.MAX_VALUE messages in one batch
    if (totalSize > queueSize || msgList.size() >= Short.MAX_VALUE)
      sendMsgList();
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
  {
    sendMsgList();
    producer.close();
  }
}
