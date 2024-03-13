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

/**
 * Represents a regular expression used to subscribe to topics. The pattern
 * must be a Google RE2/J compatible pattern. Visit
 * 
 * @see <a href="https://github.com/google/re2j">RE2/J regular expression engine</a>
 */

public class SubscriptionPattern {
    final private String pattern;
    public SubscriptionPattern(final String pattern) {
        if (pattern.equals("") || pattern == null) {
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));
        } else {
            this.pattern = pattern;
        }

    }

    public String pattern() {
        return this.pattern;
    }
}
