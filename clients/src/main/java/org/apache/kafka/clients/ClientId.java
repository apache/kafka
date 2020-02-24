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
package org.apache.kafka.clients;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * Utility to produce a client ID for consumers and producers of Kafka.
 * </p>
 *
 * <p>
 * Users may provide a custom user agent string format string to which a unique
 * number (1, 2, etc.) will be supplied as the single argument. This number
 * will be unique to the built instance of the ClientId and will be assigned
 * sequentially.
 * </p>
 *
 * <pre>
 * example: my-kafka-client-%d
 * output:  my-kafka-client-67
 * </pre>
 *
 * <p>
 * If no user agent format string is provided, the following default string is used:
 * </p>
 *
 * <pre>
 * example: Java-kafka-client-%d/{java.version}
 * output:  Java-kafka-client-67/1.8.0_232
 * </pre>
 *
 * @see String#format(String, Object...)
 */
public class ClientId {

    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    private static final String JAVA_VERSION = javaVersion();

    public static Builder newBuilder() {
        return new Builder();
    }

    static void resetSequence() {
        CONSUMER_CLIENT_ID_SEQUENCE.set(1);
    }

    private static String javaVersion() {
      PrivilegedAction<String> pa = () -> System.getProperty("java.version");
      return AccessController.doPrivileged(pa);
    }

    public static class Builder {

        private Optional<Number> sequence = Optional.empty();
        private Optional<String> userAgent = Optional.empty();
        private Optional<GroupRebalanceConfig> groupRebalanceConfig = Optional.empty();

        private Builder() {
        }

        public Builder setSequenceNumber(Number seq) {
            this.sequence = Optional.ofNullable(seq);
            return this;
        }

        public Builder setUserAgent(String ua) {
            this.userAgent = Optional.ofNullable(ua);
            return this;
        }

        public Builder setGroupRebalanceConfig(GroupRebalanceConfig grc) {
            this.groupRebalanceConfig = Optional.ofNullable(grc);
            return this;
        }

        private String defaultUserAgent() {
            return "Java-kafka-client-%d/" + JAVA_VERSION;
        }

        private String groupRebalanceUserAgent() {
            GroupRebalanceConfig rebalanceConfig = this.groupRebalanceConfig.get();

            if (rebalanceConfig.groupId != null && !rebalanceConfig.groupId.isEmpty()) {
                return "Java-kafka-client-" + rebalanceConfig.groupId + "-"
                      + rebalanceConfig.groupInstanceId.orElse("%d") + "/" + JAVA_VERSION;
            }

            return defaultUserAgent();
        }

        public String build() {
            final Number seq = (sequence.isPresent()) ? sequence.get() :
                    CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();

            final String ua;
            if (userAgent.isPresent()) {
                ua = userAgent.get();
            } else if (groupRebalanceConfig.isPresent()) {
                ua = groupRebalanceUserAgent();
            } else {
                ua = defaultUserAgent();
            }

            return String.format(ua, seq);
        }
    }
}
