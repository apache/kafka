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
package org.apache.kafka.test;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;


/**
 * Helper functions for writing unit tests
 */
public class TestUtils {

    public static final File IO_TMP_DIR = new File(System.getProperty("java.io.tmpdir"));

    public static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static final String DIGITS = "0123456789";
    public static final String LETTERS_AND_DIGITS = LETTERS + DIGITS;

    /* A consistent random number generator to make tests repeatable */
    public static final Random SEEDED_RANDOM = new Random(192348092834L);
    public static final Random RANDOM = new Random();

    public static Cluster singletonCluster(Map<String, Integer> topicPartitionCounts) {
        return clusterWith(1, topicPartitionCounts);
    }

    public static Cluster singletonCluster(String topic, int partitions) {
        return clusterWith(1, topic, partitions);
    }

    public static Cluster clusterWith(int nodes, Map<String, Integer> topicPartitionCounts) {
        Node[] ns = new Node[nodes];
        for (int i = 0; i < nodes; i++)
            ns[i] = new Node(i, "localhost", 1969);
        List<PartitionInfo> parts = new ArrayList<>();
        for (Map.Entry<String, Integer> topicPartition : topicPartitionCounts.entrySet()) {
            String topic = topicPartition.getKey();
            int partitions = topicPartition.getValue();
            for (int i = 0; i < partitions; i++)
                parts.add(new PartitionInfo(topic, i, ns[i % ns.length], ns, ns));
        }
        return new Cluster(asList(ns), parts, Collections.<String>emptySet());
    }

    public static Cluster clusterWith(int nodes, String topic, int partitions) {
        return clusterWith(nodes, Collections.singletonMap(topic, partitions));
    }

    /**
     * Generate an array of random bytes
     * 
     * @param size The size of the array
     */
    public static byte[] randomBytes(int size) {
        byte[] bytes = new byte[size];
        SEEDED_RANDOM.nextBytes(bytes);
        return bytes;
    }

    /**
     * Generate a random string of letters and digits of the given length
     * 
     * @param len The length of the string
     * @return The random string
     */
    public static String randomString(int len) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; i++)
            b.append(LETTERS_AND_DIGITS.charAt(SEEDED_RANDOM.nextInt(LETTERS_AND_DIGITS.length())));
        return b.toString();
    }

    /**
     * Create an empty file in the default temporary-file directory, using `kafka` as the prefix and `tmp` as the
     * suffix to generate its name.
     */
    public static File tempFile() throws IOException {
        File file = File.createTempFile("kafka", ".tmp");
        file.deleteOnExit();

        return file;
    }

    /**
     * Create a temporary relative directory in the default temporary-file directory with the given prefix.
     *
     * @param prefix The prefix of the temporary directory, if null using "kafka-" as default prefix
     */
    public static File tempDirectory(String prefix) throws IOException {
        return tempDirectory(null, prefix);
    }

    /**
     * Create a temporary relative directory in the specified parent directory with the given prefix.
     *
     * @param parent The parent folder path name, if null using the default temporary-file directory
     * @param prefix The prefix of the temporary directory, if null using "kafka-" as default prefix
     */
    public static File tempDirectory(Path parent, String prefix) throws IOException {
        final File file = parent == null ?
                Files.createTempDirectory(prefix == null ? "kafka-" : prefix).toFile() :
                Files.createTempDirectory(parent, prefix == null ? "kafka-" : prefix).toFile();
        file.deleteOnExit();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Utils.delete(file);
            }
        });

        return file;
    }

}
