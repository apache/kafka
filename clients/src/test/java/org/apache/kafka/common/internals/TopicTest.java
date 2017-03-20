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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertTrue;

public class TopicTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldRecognizeValidTopicNames() {
        String[] validTopicNames = {"valid", "TOPIC", "nAmEs", "ar6", "VaL1d", "_0-9_."};

        for (String topicName : validTopicNames) {
            Topic.validate(topicName);
        }
    }

    @Test
    public void shouldThrowOnInvalidTopicNames() {
        String[] invalidTopicNames = {"", "foo bar", "..", "foo:bar", "foo=bar"};

        for (String topicName : invalidTopicNames) {
            thrown.expect(InvalidTopicException.class);
            Topic.validate(topicName);
        }
    }

    @Test
    public void shouldRecognizeEmptyTopicNames() {
        assertTrue(Topic.isEmpty(""));
    }

    @Test
    public void shouldRecognizeTopicNamesThatExceedMaxLength() {
        String longName = "ATCG";

        for (int i = 0; i < 6; i++) {
            longName += longName;
        }

        assertTrue(Topic.exceedsMaxLength(longName));
    }

    @Test
    public void shouldRecognizeInvalidCharactersInTopicNames() {
        Character[] invalidChars = {'/', '\\', ',', '\u0000', ':', '"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '='};

        for (Character c : invalidChars) {
            String topicName = "Is " + c + "illegal";
            assertTrue(Topic.containsInvalidCharacters(topicName));
        }
    }

    @Test
    public void shouldRecognizeTopicNamesThatContainOnlyPeriods() {
        String[] invalidTopicNames = {".", "..", "...."};

        for (String topicName : invalidTopicNames) {
            assertTrue(Topic.containsOnlyPeriods(topicName));
        }
    }
}