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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

public class Topic {

    public static final String GROUP_METADATA_TOPIC_NAME = "__consumer_offsets";
    public static final String TRANSACTION_STATE_TOPIC_NAME = "__transaction_state";
    public static final String LEGAL_CHARS = "[a-zA-Z0-9._-]";

    private static final Set<String> INTERNAL_TOPICS = Collections.unmodifiableSet(
            Utils.mkSet(GROUP_METADATA_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME));

    private static final int MAX_NAME_LENGTH = 249;

    public static void validate(String topic) {
        validate(topic, "Topic name", message -> {
            throw new InvalidTopicException(message);
        });
    }

    public static void validate(String name, String logPrefix, Consumer<String> throwableConsumer) {
        if (name.isEmpty())
            throwableConsumer.accept(logPrefix + " is illegal, it can't be empty");
        if (".".equals(name) || "..".equals(name))
            throwableConsumer.accept(logPrefix + " cannot be \".\" or \"..\"");
        if (name.length() > MAX_NAME_LENGTH)
            throwableConsumer.accept(logPrefix + " is illegal, it can't be longer than " + MAX_NAME_LENGTH +
                    " characters, " + logPrefix + ": " + name);
        if (!containsValidPattern(name))
            throwableConsumer.accept(logPrefix + " \"" + name + "\" is illegal, it contains a character other than " +
                    "ASCII alphanumerics, '.', '_' and '-'");
    }

    public static boolean isInternal(String topic) {
        return INTERNAL_TOPICS.contains(topic);
    }

    /**
     * Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide.
     *
     * @param topic The topic to check for colliding character
     * @return true if the topic has collision characters
     */
    public static boolean hasCollisionChars(String topic) {
        return topic.contains("_") || topic.contains(".");
    }

    /**
     * Returns true if the topicNames collide due to a period ('.') or underscore ('_') in the same position.
     *
     * @param topicA A topic to check for collision
     * @param topicB A topic to check for collision
     * @return true if the topics collide
     */
    public static boolean hasCollision(String topicA, String topicB) {
        return topicA.replace('.', '_').equals(topicB.replace('.', '_'));
    }

    /**
     * Valid characters for Kafka topics are the ASCII alphanumerics, '.', '_', and '-'
     */
    static boolean containsValidPattern(String topic) {
        for (int i = 0; i < topic.length(); ++i) {
            char c = topic.charAt(i);

            // We don't use Character.isLetterOrDigit(c) because it's slower
            boolean validChar = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || c == '.' ||
                    c == '_' || c == '-';
            if (!validChar)
                return false;
        }
        return true;
    }
}
