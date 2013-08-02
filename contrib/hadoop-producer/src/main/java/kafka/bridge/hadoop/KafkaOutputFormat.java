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
import java.util.*;

import kafka.common.KafkaException;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

public class KafkaOutputFormat<K, V> extends OutputFormat<K, V>
{
  private Logger log = Logger.getLogger(KafkaOutputFormat.class);

  public static final String KAFKA_URL = "kafka.output.url";
  /** Bytes to buffer before the OutputFormat does a send (i.e., the amortization window):
   *  We set the default to a million bytes so that the server will not reject the batch of messages
   *  with a MessageSizeTooLargeException. The actual size will be smaller after compression.
   */
  public static final int KAFKA_QUEUE_BYTES = 1000000;

  public static final String KAFKA_CONFIG_PREFIX = "kafka.output";
  private static final Map<String, String> kafkaConfigMap;
  static {
    Map<String, String> cMap = new HashMap<String, String>();

    // default Hadoop producer configs
    cMap.put("producer.type", "sync");
    cMap.put("compression.codec", Integer.toString(1));
    cMap.put("request.required.acks", Integer.toString(1));

    kafkaConfigMap = Collections.unmodifiableMap(cMap);
  }

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
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException
  {
    Path outputPath = getOutputPath(context);
    if (outputPath == null)
      throw new KafkaException("no kafka output url specified");
    URI uri = URI.create(outputPath.toString());
    Configuration job = context.getConfiguration();

    Properties props = new Properties();
    String topic;

    props.putAll(kafkaConfigMap);                       // inject default configuration
    for (Map.Entry<String, String> m : job) {           // handle any overrides
      if (!m.getKey().startsWith(KAFKA_CONFIG_PREFIX))
        continue;
      if (m.getKey().equals(KAFKA_URL))
        continue;

      String kafkaKeyName = m.getKey().substring(KAFKA_CONFIG_PREFIX.length()+1);
      props.setProperty(kafkaKeyName, m.getValue());    // set Kafka producer property
    }

    // inject Kafka producer props back into jobconf for easier debugging
    for (Map.Entry<Object, Object> m : props.entrySet()) {
      job.set(KAFKA_CONFIG_PREFIX + "." + m.getKey().toString(), m.getValue().toString());
    }

    // KafkaOutputFormat specific parameters
    final int queueBytes = job.getInt(KAFKA_CONFIG_PREFIX + ".queue.bytes", KAFKA_QUEUE_BYTES);

    if (uri.getScheme().equals("kafka")) {
      // using the direct broker list
      // URL: kafka://<kafka host>/<topic>
      // e.g. kafka://kafka-server:9000,kafka-server2:9000/foobar
      String brokerList = uri.getAuthority();
      props.setProperty("metadata.broker.list", brokerList);
      job.set(KAFKA_CONFIG_PREFIX + ".metadata.broker.list", brokerList);

      if (uri.getPath() == null || uri.getPath().length() <= 1)
        throw new KafkaException("no topic specified in kafka uri");

      topic = uri.getPath().substring(1);               // ignore the initial '/' in the path
      job.set(KAFKA_CONFIG_PREFIX + ".topic", topic);
      log.info(String.format("using kafka broker %s (topic %s)", brokerList, topic));
    } else
      throw new KafkaException("missing scheme from kafka uri (must be kafka://)");

    Producer<Object, byte[]> producer = new Producer<Object, byte[]>(new ProducerConfig(props));
    return new KafkaRecordWriter<K, V>(producer, topic, queueBytes);
  }
}
