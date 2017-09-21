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
package org.apache.kafka.common.metrics;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.KafkaException;

/**
 * Utility class for sanitizing/desanitizing user principal and client-ids
 * to a safe value for use in MetricName and as Zookeeper node name
 */
public class Sanitizer {

    public static String sanitize(String name) {
        String encoded = "";
        try {
            encoded = URLEncoder.encode(name, StandardCharsets.UTF_8.name());
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < encoded.length(); i++) {
                char c = encoded.charAt(i);
                if (c == '*') {         // Metric ObjectName treats * as pattern
                    builder.append("%2A");
                } else if (c == '+') {  // Space URL-encoded as +, replace with percent encoding
                    builder.append("%20");
                } else {
                    builder.append(c);
                }
            }
            return builder.toString();
        } catch (UnsupportedEncodingException e) {
            throw new KafkaException(e);
        }
    }

    public static String desanitize(String name) {
        try {
            return URLDecoder.decode(name, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new KafkaException(e);
        }
    }

}
