/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.support.metrics.tools;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.confluent.support.metrics.common.time.TimeUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class KafkaMetricsToFile {

  private final String bootstrapServer;

  /**
   * Default constructor
   *
   * @param bootstrapServer Kafka broker to connect to e.g., localhost:9092
   */
  public KafkaMetricsToFile(String bootstrapServer) {
    this.bootstrapServer = bootstrapServer;
  }

  /**
   * Retrieves the metrics from the provided topic and stores them in a compressed local file.
   *
   * @param topic Kafka topic to read from.  Must not be null or empty.
   * @param outputPath Path to the output file.  Must not be null or empty.
*    @param runTimeMs Time this script should run for in milliseconds
   * @return the number of retrieved metrics submissions.
   */
  public int saveMetricsToFile(String topic, String outputPath, int runTimeMs) {
    if (topic == null || topic.isEmpty()) {
      System.err.println("Topic name must be specified");
      return 0;
    }
    if (outputPath == null || outputPath.isEmpty()) {
      System.err.println("Output path must be specified");
      return 0;
    }

    long endTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) + runTimeMs;
    int numMessages = 0;
    File outFile = new File(outputPath);
    try (FileOutputStream fOut = new FileOutputStream(outFile);
         BufferedOutputStream bOut = new BufferedOutputStream(fOut);
         ZipArchiveOutputStream zOut = new ZipArchiveOutputStream(bOut);
         KafkaConsumer<byte[], byte[]> consumer = createConsumer()) {

      consumer.subscribe(Collections.singleton(topic));

      long timeRemainingMs = runTimeMs;
      while (timeRemainingMs > 0) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(timeRemainingMs));
        for (ConsumerRecord<byte[], byte[]> record : records) {
          ZipArchiveEntry entry = new ZipArchiveEntry(outputPath + "." + numMessages);
          zOut.putArchiveEntry(entry);
          zOut.write(record.value());
          zOut.closeArchiveEntry();

          System.out.println("Collecting metric batch #" + numMessages);
          numMessages++;
        }
        timeRemainingMs = endTimeMs - TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
      }
      System.out.println("Collection completed.");
    } catch (FileNotFoundException e) {
      System.err.println("File not found: " + e.getMessage());
      return 0;
    } catch (IOException e) {
      System.err.println("IOException: " + e.getMessage());
      return 0;
    }

    if (numMessages == 0) {
      try {
        Files.deleteIfExists(outFile.toPath());
      } catch (IOException e) {
        System.out.println("Unable to delete " + outFile + ": " + e.getMessage());
      }
      System.out.println("No records found.");
    } else {
      System.out.println("Created file " + outputPath + " with " + numMessages + " records");
    }

    return numMessages;
  }

  // Visible for testing
  public KafkaConsumer<byte[], byte[]> createConsumer() {
    long unixTime = new TimeUtils().nowInUnixTime();
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
            "KafkaSupportGroup-" + unixTime + "-" + new Random().nextInt(100000));
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "25000");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Usage: bootstrapServer topic outputFile runtimeSecs");
      return;
    }
    String bootstrapServer = args[0];
    String topic = args[1];
    String outputPath = args[2];
    int runtimeSeconds = Integer.parseInt(args[3]);
    int runTimeMs = runtimeSeconds * 1000;
    System.out.print("Collecting metrics. This might take up to " + runtimeSeconds + " seconds.");

    KafkaMetricsToFile kafkaMetricsToFile = new KafkaMetricsToFile(bootstrapServer);
    kafkaMetricsToFile.saveMetricsToFile(topic, outputPath, runTimeMs);
  }
}
