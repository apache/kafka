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
package kafka.bridge.examples;

import java.io.IOException;
import kafka.bridge.hadoop.KafkaOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Publish a text file line by line to a Kafka topic
 */
public class TextPublisher
{
  public static void main(String[] args) throws Exception
  {
    if (args.length != 2) {
      System.err.println("usage: <input path> <kafka output url>");
      return;
    }

    Job job = new Job();

    job.setJarByClass(TextPublisher.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(KafkaOutputFormat.class);

    job.setMapperClass(TheMapper.class);
    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    KafkaOutputFormat.setOutputPath(job, new Path(args[1]));

    if (!job.waitForCompletion(true)) {
      throw new RuntimeException("Job failed!");
    }
  }

  public static class TheMapper extends Mapper<Object, Text, Object, Object>
  {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      context.write(null, value.getBytes());
    }
  }
}

