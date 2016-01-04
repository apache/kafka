/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

public interface KafkaProperties {
    String ZK_CONNECT = "127.0.0.1:2181";
    String GROUP_ID = "group1";
    String TOPIC = "topic1";
    String KAFKA_SERVER_URL = "localhost";
    int KAFKA_SERVER_PORT = 9092;
    int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    int CONNECTION_TIMEOUT = 100000;
    int RECONNECT_INTERVAL = 10000;
    String TOPIC2 = "topic2";
    String TOPIC3 = "topic3";
    String CLIENT_ID = "SimpleConsumerDemoClient";
}
