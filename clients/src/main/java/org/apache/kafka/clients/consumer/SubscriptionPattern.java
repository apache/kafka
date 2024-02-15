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
 * A class that hold a regular expression which compatible with Google's RE2J engine. Visit
 * <a href="https://github.com/google/re2j">this repository</a> for details on RE2J engine.
 */

public class SubscriptionPattern {
    final private String pattern;
    public SubscriptionPattern(final String pattern) {
        this.pattern = pattern;
    }

    public String pattern() {
        return this.pattern;
    }
}
