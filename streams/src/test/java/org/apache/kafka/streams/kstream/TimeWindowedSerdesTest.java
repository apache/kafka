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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

public class TimeWindowedSerdesTest {

    final private String key = "key";
    final private String topic = "topic";
    final private long startTime = 50L;
    final private long endTime = 100L;
    final private Serde<String> serde = Serdes.String();

    final private Window window = new TimeWindow(startTime, endTime);
    final private Windowed<String> windowedKey = new Windowed<>(key, window);
    final private Serde<Windowed<String>> keySerde = new WindowedSerdes.TimeWindowedSerde<>(serde);


}
