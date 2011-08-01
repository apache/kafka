/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.Properties;

import kafka.javaapi.producer.SyncProducer;
import kafka.producer.SyncProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.net.URI;

public class KafkaOutputFormat<W extends BytesWritable> extends OutputFormat<NullWritable, W>
{
  public static final String KAFKA_URL = "kafka.output.url";
  public static final int KAFKA_PRODUCER_CONNECT_TIMEOUT = 30*1000;
  public static final int KAFKA_PRODUCER_RECONNECT_INTERVAL = 1000;
  public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64*1024;
  public static final int KAFKA_PRODUCER_MAX_MESSAGE_SIZE = 1024*1024;
  public static final int KAFKA_QUEUE_SIZE = 10*1024*1024;

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
    URI uri = outputPath.toUri();
    Configuration job = context.getConfiguration();

    final String topic = uri.getPath().substring(1);        // ignore the initial '/' in the path

    final int queueSize = job.getInt("kafka.output.queue_size", KAFKA_QUEUE_SIZE);
    final int timeout = job.getInt("kafka.output.connect_timeout", KAFKA_PRODUCER_CONNECT_TIMEOUT);
    final int interval = job.getInt("kafka.output.reconnect_interval", KAFKA_PRODUCER_RECONNECT_INTERVAL);
    final int bufSize = job.getInt("kafka.output.bufsize", KAFKA_PRODUCER_BUFFER_SIZE);
    final int maxSize = job.getInt("kafka.output.max_msgsize", KAFKA_PRODUCER_MAX_MESSAGE_SIZE);

    job.set("kafka.output.server", String.format("%s:%d", uri.getHost(), uri.getPort()));
    job.set("kafka.output.topic", topic);
    job.setInt("kafka.output.queue_size", queueSize);
    job.setInt("kafka.output.connect_timeout", timeout);
    job.setInt("kafka.output.reconnect_interval", interval);
    job.setInt("kafka.output.bufsize", bufSize);
    job.setInt("kafka.output.max_msgsize", maxSize);

    if (uri.getHost().isEmpty())
      throw new IllegalArgumentException("missing kafka server");
    if (uri.getPath().isEmpty())
      throw new IllegalArgumentException("missing kafka topic");

    Properties props = new Properties();
    props.setProperty("host", uri.getHost());
    props.setProperty("port", Integer.toString(uri.getPort()));
    props.setProperty("buffer.size", Integer.toString(bufSize));
    props.setProperty("connect.timeout.ms", Integer.toString(timeout));
    props.setProperty("reconnect.interval", Integer.toString(interval));
    props.setProperty("max.message.size", Integer.toString(maxSize));

    SyncProducer producer = new SyncProducer(new SyncProducerConfig(props));
    return new KafkaRecordWriter<W>(producer, topic, queueSize);
  }
}

