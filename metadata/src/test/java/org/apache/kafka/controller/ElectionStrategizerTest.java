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

package org.apache.kafka.controller;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ElectionStrategizerTest {
    private final static Logger log = LoggerFactory.getLogger(ElectionStrategizerTest.class);

    private static ElectionStrategizer createElectionStrategizer() {
        return new ElectionStrategizer(0).
            setTopicUncleanConfigAccessor(n -> {
                if (n.startsWith("unclean_")) {
                    return "true";
                } else if (n.startsWith("clean_")) {
                    return "false";
                } else {
                    return null;
                }
            }).
            setNodeUncleanConfig(null).
            setClusterUncleanConfig("true");
    }

    @Test
    public void testShouldBeUnclean() {
        ElectionStrategizer strategizer = createElectionStrategizer();
        assertTrue(strategizer.shouldBeUnclean("unclean_foo"));
        assertFalse(strategizer.shouldBeUnclean("clean_foo"));
        assertTrue(strategizer.shouldBeUnclean("foo"));
        strategizer.setNodeUncleanConfig("false");
        assertFalse(strategizer.shouldBeUnclean("foo"));
    }

    @Test
    public void testTopicUncleanOverride() {
        ElectionStrategizer strategizer = createElectionStrategizer();
        strategizer.setTopicUncleanOverride("unclean_foo", "false");
        strategizer.setTopicUncleanOverride("clean_bar", "true");
        assertFalse(strategizer.shouldBeUnclean("unclean_foo"));
        assertTrue(strategizer.shouldBeUnclean("clean_bar"));
    }

    @Test
    public void testParseBoolean() {
        ElectionStrategizer strategizer = createElectionStrategizer();
        assertFalse(strategizer.parseBoolean("testParseBoolean", "false"));
        assertFalse(strategizer.parseBoolean("testParseBoolean", "FALSE"));
        assertTrue(strategizer.parseBoolean("testParseBoolean", "true"));
        assertTrue(strategizer.parseBoolean("testParseBoolean", "TRUE"));
        assertNull(strategizer.parseBoolean("testParseBoolean", ""));
        assertNull(strategizer.parseBoolean("testParseBoolean", " "));
        assertNull(strategizer.parseBoolean("testParseBoolean", null));
        assertNull(strategizer.parseBoolean("testParseBoolean", "foo"));
    }
}
