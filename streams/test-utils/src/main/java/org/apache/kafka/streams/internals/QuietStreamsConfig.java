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
package org.apache.kafka.streams.internals;

import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;

/**
 * A {@link StreamsConfig} that does not log its configuration on construction.
 *
 * This producer cleaner output for unit tests using the {@code test-utils},
 * since logging the config is not really valuable in this context.
 */
public class QuietStreamsConfig extends StreamsConfig {
    public QuietStreamsConfig(final Map<?, ?> props) {
        super(props, false);
    }
}
