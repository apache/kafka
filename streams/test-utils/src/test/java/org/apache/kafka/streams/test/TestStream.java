/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.test;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;

/**
 * Simple Test Stream Skeleton
 * @author Jukka Karvanen / jukinimi.com
 */
class TestStream {
    final static String INPUT_TOPIC = "input";
    final static String OUTPUT_TOPIC="output";

    final Properties config = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "TestInputOutputTopicTest"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    ));

    void createStream(final StreamsBuilder builder) {
      builder.stream(INPUT_TOPIC).to(OUTPUT_TOPIC);
    }

  }