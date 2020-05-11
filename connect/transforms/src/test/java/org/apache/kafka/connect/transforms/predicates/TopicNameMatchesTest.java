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
package org.apache.kafka.connect.transforms.predicates;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TopicNameMatchesTest {

    @Test
    public void testConfig() {
        TopicNameMatches<SourceRecord> predicate = new TopicNameMatches<>();
        predicate.config().validate(Collections.singletonMap("pattern", "my-prefix-.*"));

        List<ConfigValue> configs = predicate.config().validate(Collections.singletonMap("pattern", "*"));
        assertEquals(singletonList("Invalid value * for configuration pattern: " +
                        "entry must be a Java-compatible regular expression: " +
                        "Dangling meta character '*' near index 0" + System.lineSeparator() +
                        "*" + System.lineSeparator() +
                        "^"),
                configs.get(0).errorMessages());
    }

    @Test
    public void testTest() {
        TopicNameMatches<SourceRecord> predicate = new TopicNameMatches<>();
        predicate.configure(Collections.singletonMap("pattern", "my-prefix-.*"));

        assertTrue(predicate.test(recordWithTopicName("my-prefix-")));
        assertTrue(predicate.test(recordWithTopicName("my-prefix-foo")));
        assertFalse(predicate.test(recordWithTopicName("x-my-prefix-")));
        assertFalse(predicate.test(recordWithTopicName("x-my-prefix-foo")));
        assertFalse(predicate.test(recordWithTopicName("your-prefix-")));
        assertFalse(predicate.test(recordWithTopicName("your-prefix-foo")));
        assertFalse(predicate.test(new SourceRecord(null, null, null, null, null)));

    }

    private SourceRecord recordWithTopicName(String topicName) {
        return new SourceRecord(null, null, topicName, null, null);
    }
}
