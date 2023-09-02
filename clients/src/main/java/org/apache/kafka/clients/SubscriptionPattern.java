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


import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

public class SubscriptionPattern {
    final private String pattern;

    /**
     * local variable to avoid multiple compile
     */
    private Pattern complitedPattern;

    public SubscriptionPattern(final String pattern) {
        this.pattern = pattern;
    }
    public String pattern() {
        return this.pattern;
    }
    public boolean match(String subject) {
        if (!isValidPattern()) {
            return false;
        }
        return complitedPattern.matcher(subject).find();
    }

    public boolean isValidPattern() {
        synchronized (this) {
            if (complitedPattern == null) {
                try {
                    this.complitedPattern = Pattern.compile(pattern);
                } catch (PatternSyntaxException e) {
                    return false;
                }
            }
        }
        return true;
    }

}
