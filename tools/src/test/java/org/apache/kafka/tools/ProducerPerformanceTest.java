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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ProducerPerformanceTest {

    @Mock
    KafkaProducer<byte[], byte[]> producerMock;

    @Spy
    ProducerPerformance producerPerformanceSpy;

    private File createTempFile(String contents) throws IOException {
        File file = File.createTempFile("ProducerPerformanceTest", ".tmp");
        file.deleteOnExit();
        final FileWriter writer = new FileWriter(file);
        writer.write(contents);
        writer.close();
        return file;
    }

    @Test
    public void testReadPayloadFile() throws Exception {
        File payloadFile = createTempFile("Hello\nKafka");
        String payloadFilePath = payloadFile.getAbsolutePath();
        String payloadDelimiter = "\n";

        List<byte[]> payloadByteList = ProducerPerformance.readPayloadFile(payloadFilePath, payloadDelimiter);

        assertEquals(2, payloadByteList.size());
        assertEquals("Hello", new String(payloadByteList.get(0)));
        assertEquals("Kafka", new String(payloadByteList.get(1)));
    }

    @Test
    public void testReadProps() throws Exception {
        
        List<String> producerProps = Collections.singletonList("bootstrap.servers=localhost:9000");
        String producerConfig = createTempFile("acks=1").getAbsolutePath();
        String transactionalId = "1234";
        boolean transactionsEnabled = true;

        Properties prop = ProducerPerformance.readProps(producerProps, producerConfig, transactionalId, transactionsEnabled);

        assertNotNull(prop);
        assertEquals(5, prop.size());
    }

    @Test
    public void testNumberOfCallsForSendAndClose() throws IOException {

        doReturn(null).when(producerMock).send(any(), any());
        doReturn(producerMock).when(producerPerformanceSpy).createKafkaProducer(any(Properties.class));

        String[] args = new String[] {"--topic", "Hello-Kafka", "--num-records", "5", "--throughput", "100", "--record-size", "100", "--producer-props", "bootstrap.servers=localhost:9000"};
        producerPerformanceSpy.start(args);
        verify(producerMock, times(5)).send(any(), any());
        verify(producerMock, times(1)).close();
    }

    @Test
    public void testUnexpectedArg() {

        String[] args = new String[] {"--test", "test", "--topic", "Hello-Kafka", "--num-records", "5", "--throughput", "100", "--record-size", "100", "--producer-props", "bootstrap.servers=localhost:9000"};
        ArgumentParser parser = ProducerPerformance.argParser();
        ArgumentParserException thrown = assertThrows(ArgumentParserException.class, () -> parser.parseArgs(args));
        assertEquals("unrecognized arguments: '--test'", thrown.getMessage());
    }

    @Test
    public void testGenerateRandomPayloadByPayloadFile() {

        Integer recordSize = null;
        boolean hasPayloadFile = true;

        String inputString = "Hello Kafka";
        byte[] byteArrray = inputString.getBytes(StandardCharsets.UTF_8);
        List<byte[]> payloadByteList = new ArrayList<>();
        payloadByteList.add(byteArrray);
    
        byte[] payload = null;
        Random random = new Random(0);
        payload = ProducerPerformance.generateRandomPayload(recordSize, hasPayloadFile, payloadByteList, payload, random);
        assertEquals("Hello Kafka", new String(payload));
    }

    @Test
    public void testGenerateRandomPayloadByRecordSize() throws UnsupportedEncodingException {

        Integer recordSize = 100;
        byte[] payload = new byte[recordSize];

        boolean hasPayloadFile = false;
        List<byte[]> payloadByteList = null;
    
        Random random = new Random(0);
        payload = ProducerPerformance.generateRandomPayload(recordSize, hasPayloadFile, payloadByteList, payload, random);
        
        for (int i = 0; i < payload.length; i++) {
            assertFalse(payload[i] == 0);
        }
    }

    @Test
    public void testGenerateRandomPayloadException() throws UnsupportedEncodingException {

        Integer recordSize = null;
        byte[] payload = null;

        boolean hasPayloadFile = false;
        List<byte[]> payloadByteList = null;
    
        Random random = new Random(0);
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ProducerPerformance.generateRandomPayload(recordSize, hasPayloadFile, payloadByteList, payload, random));
        assertEquals("no payload File Path or record Size provided", thrown.getMessage());
    }
}
