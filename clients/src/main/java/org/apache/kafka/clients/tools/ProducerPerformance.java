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
package org.apache.kafka.clients.tools;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.Records;


public class ProducerPerformance {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("USAGE: java " + ProducerPerformance.class.getName() + " url num_records record_size");
            System.exit(1);
        }
        String url = args[0];
        int numRecords = Integer.parseInt(args[1]);
        int recordSize = Integer.parseInt(args[2]);
        Properties props = new Properties();
        props.setProperty(ProducerConfig.REQUIRED_ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.BROKER_LIST_CONFIG, url);
        props.setProperty(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, Integer.toString(5 * 1000));
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_CONFIG, Integer.toString(Integer.MAX_VALUE));

        KafkaProducer producer = new KafkaProducer(props);
        Callback callback = new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null)
                    e.printStackTrace();
            }
        };
        byte[] payload = new byte[recordSize];
        Arrays.fill(payload, (byte) 1);
        ProducerRecord record = new ProducerRecord("test", payload);
        long start = System.currentTimeMillis();
        long maxLatency = -1L;
        long totalLatency = 0;
        int reportingInterval = 1000000;
        for (int i = 0; i < numRecords; i++) {
            long sendStart = System.currentTimeMillis();
            producer.send(record, callback);
            long sendEllapsed = System.currentTimeMillis() - sendStart;
            maxLatency = Math.max(maxLatency, sendEllapsed);
            totalLatency += sendEllapsed;
            if (i % reportingInterval == 0) {
                System.out.printf("%d  max latency = %d ms, avg latency = %.5f\n",
                                  i,
                                  maxLatency,
                                  (totalLatency / (double) reportingInterval));
                totalLatency = 0L;
                maxLatency = -1L;
            }
        }
        long ellapsed = System.currentTimeMillis() - start;
        double msgsSec = 1000.0 * numRecords / (double) ellapsed;
        double mbSec = msgsSec * (recordSize + Records.LOG_OVERHEAD) / (1024.0 * 1024.0);
        System.out.printf("%d records sent in %d ms ms. %.2f records per second (%.2f mb/sec).", numRecords, ellapsed, msgsSec, mbSec);
        producer.close();
    }

}
