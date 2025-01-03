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
package org.apache.kafka.clients.consumer;

import java.util.Objects;

/**
 * Represents a regular expression compatible with Google RE2/J, used to subscribe to topics.
 * This just keeps the String representation of the pattern, and all validations to ensure
 * it is RE2/J compatible are delegated to the broker.
 */
public class SubscriptionPattern {

    /**
     * String representation the regular expression, compatible with RE2/J.
     */
    private final String pattern;

    public SubscriptionPattern(String pattern) {
        this.pattern = pattern;
    }

    /**
     * @return Regular expression pattern compatible with RE2/J.
     */
    public String pattern() {
        return this.pattern;
    }

    @Override
    public String toString() {
        return pattern;
    }

    @Override
    public int hashCode() {
        return pattern.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SubscriptionPattern &&
            Objects.equals(pattern, ((SubscriptionPattern) obj).pattern);
    }
}
