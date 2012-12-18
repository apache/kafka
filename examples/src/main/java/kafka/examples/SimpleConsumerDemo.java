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
package kafka.examples;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import java.util.HashMap;
import java.util.Map;

public class SimpleConsumerDemo {
    
  private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
    for(MessageAndOffset messageAndOffset: messageSet) {
      ByteBuffer payload = messageAndOffset.message().payload();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      System.out.println(new String(bytes, "UTF-8"));
    }
  }

  private static void generateData() {
    Producer producer2 = new Producer(KafkaProperties.topic2);
    producer2.start();
    Producer producer3 = new Producer(KafkaProperties.topic3);
    producer3.start();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args) throws Exception {
    generateData();
      
    SimpleConsumer simpleConsumer = new SimpleConsumer(KafkaProperties.kafkaServerURL,
                                                       KafkaProperties.kafkaServerPort,
                                                       KafkaProperties.connectionTimeOut,
                                                       KafkaProperties.kafkaProducerBufferSize,
                                                       KafkaProperties.clientId);

    System.out.println("Testing single fetch");
    FetchRequest req = new FetchRequestBuilder()
            .clientId(KafkaProperties.clientId)
            .addFetch(KafkaProperties.topic2, 0, 0L, 100)
            .build();
    FetchResponse fetchResponse = simpleConsumer.fetch(req);
      printMessages((ByteBufferMessageSet) fetchResponse.messageSet(KafkaProperties.topic2, 0));

    System.out.println("Testing single multi-fetch");
    Map<String, List<Integer>> topicMap = new HashMap<String, List<Integer>>() {{
        put(KafkaProperties.topic2, new ArrayList<Integer>(){{ add(0); }});
        put(KafkaProperties.topic3, new ArrayList<Integer>(){{ add(0); }});
    }};
    req = new FetchRequestBuilder()
            .clientId(KafkaProperties.clientId)
            .addFetch(KafkaProperties.topic2, 0, 0L, 100)
            .addFetch(KafkaProperties.topic3, 0, 0L, 100)
            .build();
    fetchResponse = simpleConsumer.fetch(req);
    int fetchReq = 0;
    for ( Map.Entry<String, List<Integer>> entry : topicMap.entrySet() ) {
      String topic = entry.getKey();
      for ( Integer offset : entry.getValue()) {
        System.out.println("Response from fetch request no: " + ++fetchReq);
        printMessages((ByteBufferMessageSet) fetchResponse.messageSet(topic, offset));
      }
    }
  }
}
