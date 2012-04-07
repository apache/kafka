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
import java.net.URI;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

public class KafkaOutputFormat<W extends BytesWritable> extends OutputFormat<NullWritable, W>
{
  private Logger log = Logger.getLogger(KafkaOutputFormat.class);

  public static final String KAFKA_URL = "kafka.output.url";
  /** Bytes to buffer before the OutputFormat does a send */
  public static final int KAFKA_QUEUE_SIZE = 10*1024*1024;

  /** Default value for Kafka's connect.timeout.ms */
  public static final int KAFKA_PRODUCER_CONNECT_TIMEOUT = 30*1000;
  /** Default value for Kafka's reconnect.interval*/
  public static final int KAFKA_PRODUCER_RECONNECT_INTERVAL = 1000;
  /** Default value for Kafka's buffer.size */
  public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64*1024;
  /** Default value for Kafka's max.message.size */
  public static final int KAFKA_PRODUCER_MAX_MESSAGE_SIZE = 1024*1024;
  /** Default value for Kafka's producer.type */
  public static final String KAFKA_PRODUCER_PRODUCER_TYPE = "sync";
  /** Default value for Kafka's compression.codec */
  public static final int KAFKA_PRODUCER_COMPRESSION_CODEC = 0;

  public KafkaOutputFormat()
  {
    super();
  }

  public static void setOutputPath(Job job, Path outputUrl)
  {
    job.getConfiguration().set(KafkaOutputFormat.KAFKA_URL, outputUrl.toString());

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
  }

  public static Path getOutputPath(JobContext job)
  {
    String name = job.getConfiguration().get(KafkaOutputFormat.KAFKA_URL);
    return name == null ? null : new Path(name);
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException
  {
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
  {
    // Is there a programmatic way to get the temp dir? I see it hardcoded everywhere in Hadoop, Hive, and Pig.
    return new FileOutputCommitter(new Path("/tmp/" + taskAttemptContext.getTaskAttemptID().getJobID().toString()), taskAttemptContext);
  }

  @Override
  public RecordWriter<NullWritable, W> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException
  {
    Path outputPath = getOutputPath(context);
    if (outputPath == null)
      throw new IllegalArgumentException("no kafka output url specified");
    URI uri = URI.create(outputPath.toString());
    Configuration job = context.getConfiguration();

    Properties props = new Properties();
    String topic;

    final int queueSize = job.getInt("kafka.output.queue_size", KAFKA_QUEUE_SIZE);
    final int timeout = job.getInt("kafka.output.connect_timeout", KAFKA_PRODUCER_CONNECT_TIMEOUT);
    final int interval = job.getInt("kafka.output.reconnect_interval", KAFKA_PRODUCER_RECONNECT_INTERVAL);
    final int bufSize = job.getInt("kafka.output.bufsize", KAFKA_PRODUCER_BUFFER_SIZE);
    final int maxSize = job.getInt("kafka.output.max_msgsize", KAFKA_PRODUCER_MAX_MESSAGE_SIZE);
    final String producerType = job.get("kafka.output.producer_type", KAFKA_PRODUCER_PRODUCER_TYPE);
    final int compressionCodec = job.getInt("kafka.output.compression_codec", KAFKA_PRODUCER_COMPRESSION_CODEC);

    job.setInt("kafka.output.queue_size", queueSize);
    job.setInt("kafka.output.connect_timeout", timeout);
    job.setInt("kafka.output.reconnect_interval", interval);
    job.setInt("kafka.output.bufsize", bufSize);
    job.setInt("kafka.output.max_msgsize", maxSize);
    job.set("kafka.output.producer_type", producerType);
    job.setInt("kafka.output.compression_codec", compressionCodec);

    props.setProperty("producer.type", producerType);
    props.setProperty("buffer.size", Integer.toString(bufSize));
    props.setProperty("connect.timeout.ms", Integer.toString(timeout));
    props.setProperty("reconnect.interval", Integer.toString(interval));
    props.setProperty("max.message.size", Integer.toString(maxSize));
    props.setProperty("compression.codec", Integer.toString(compressionCodec));

    if (uri.getScheme().equals("kafka+zk")) {
      // Software load balancer:
      //  URL: kafka+zk://<zk connect path>#<kafka topic>
      //  e.g. kafka+zk://kafka-zk:2181/kafka#foobar

      String zkConnect = uri.getAuthority() + uri.getPath();

      props.setProperty("zk.connect", zkConnect);
      job.set("kafka.zk.connect", zkConnect);

      topic = uri.getFragment();
      if (topic == null)
        throw new IllegalArgumentException("no topic specified in kafka uri fragment");

      log.info(String.format("using kafka zk.connect %s (topic %s)", zkConnect, topic));
    } else if (uri.getScheme().equals("kafka")) {
      // using the legacy direct broker list
      // URL: kafka://<kafka host>/<topic>
      // e.g. kafka://kafka-server:9000,kafka-server2:9000/foobar

      // Just enumerate broker_ids, as it really doesn't matter what they are as long as they're unique
      // (KAFKA-258 will remove the broker_id requirement)
      StringBuilder brokerListBuilder = new StringBuilder();
      String delim = "";
      int brokerId = 0;
      for (String serverPort : uri.getAuthority().split(",")) {
        brokerListBuilder.append(delim).append(String.format("%d:%s", brokerId, serverPort));
        delim = ",";
        brokerId++;
      }
      String brokerList = brokerListBuilder.toString();

      props.setProperty("broker.list", brokerList);
      job.set("kafka.broker.list", brokerList);

      if (uri.getPath() == null || uri.getPath().length() <= 1)
        throw new IllegalArgumentException("no topic specified in kafka uri");

      topic = uri.getPath().substring(1);             // ignore the initial '/' in the path
      job.set("kafka.output.topic", topic);
      log.info(String.format("using kafka broker %s (topic %s)", brokerList, topic));
    } else
      throw new IllegalArgumentException("missing scheme from kafka uri (must be kafka:// or kafka+zk://)");

    Producer<Integer, Message> producer = new Producer<Integer, Message>(new ProducerConfig(props));
    return new KafkaRecordWriter<W>(producer, topic, queueSize);
  }
}
