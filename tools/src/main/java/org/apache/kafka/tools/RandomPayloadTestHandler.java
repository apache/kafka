/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.tools;

import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A test handler that could inject random payload records.
 */
class RandomPayloadTestHandler implements ClientTestHandler<byte[], byte[]> {

    private final String payloadFilePath;
    private final List<byte[]> payloadByteList;
    private final String outputTopic;
    private byte[] payload;
    private final Random random;
    private final Producer<byte[], byte[]> producer;

    RandomPayloadTestHandler(Namespace res, Producer<byte[], byte[]> producer, String outputTopic) throws IOException {
        Integer recordSize = res.getInt("recordSize");
        random = new Random(0);
        if (recordSize != null) {
            payload = new byte[recordSize];
            for (int i = 0; i < payload.length; ++i)
                payload[i] = (byte) (random.nextInt(26) + 65);
        }

        payloadFilePath = res.getString("payloadFile");

        // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
        String payloadDelimiter = res.getString("payloadDelimiter").equals("\\n") ? "\n" : res.getString("payloadDelimiter");
        this.outputTopic = outputTopic;

        payloadByteList = new ArrayList<>();
        if (payloadFilePath != null) {
            Path path = Paths.get(payloadFilePath);
            System.out.println("Reading payloads from: " + path.toAbsolutePath());
            if (Files.notExists(path) || Files.size(path) == 0)  {
                throw new IllegalArgumentException("File does not exist or empty file provided.");
            }

            String[] payloadList = new String(Files.readAllBytes(path), StandardCharsets.UTF_8).split(payloadDelimiter);

            System.out.println("Number of messages read: " + payloadList.length);

            for (String payload : payloadList) {
                payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
            }
        }
        this.producer = producer;
    }

    @Override
    public ProducerRecord<byte[], byte[]> getRecord() {
        if (payloadFilePath != null) {
            payload = payloadByteList.get(random.nextInt(payloadByteList.size()));
        }
        return new ProducerRecord<>(outputTopic, payload);
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void commitTransaction() {
        producer.commitTransaction();
    }

    @Override
    public void close() {
    }
}
