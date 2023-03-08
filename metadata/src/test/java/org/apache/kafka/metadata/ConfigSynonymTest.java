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

package org.apache.kafka.metadata;

import org.apache.kafka.server.config.ConfigSynonym;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ConfigSynonymTest {
    @Test
    public void testHoursToMilliseconds() {
        assertEquals("0", ConfigSynonym.HOURS_TO_MILLISECONDS.apply(""));
        assertEquals("0", ConfigSynonym.HOURS_TO_MILLISECONDS.apply(" "));
        assertEquals("0", ConfigSynonym.HOURS_TO_MILLISECONDS.apply("0"));
        assertEquals("442800000", ConfigSynonym.HOURS_TO_MILLISECONDS.apply("123"));
        assertEquals("442800000", ConfigSynonym.HOURS_TO_MILLISECONDS.apply(" 123 "));
        assertEquals("0", ConfigSynonym.HOURS_TO_MILLISECONDS.apply("not_a_number"));
    }

    @Test
    public void testMinutesToMilliseconds() {
        assertEquals("0", ConfigSynonym.MINUTES_TO_MILLISECONDS.apply(""));
        assertEquals("0", ConfigSynonym.MINUTES_TO_MILLISECONDS.apply(" "));
        assertEquals("0", ConfigSynonym.MINUTES_TO_MILLISECONDS.apply("0"));
        assertEquals("7380000", ConfigSynonym.MINUTES_TO_MILLISECONDS.apply("123"));
        assertEquals("7380000", ConfigSynonym.MINUTES_TO_MILLISECONDS.apply(" 123 "));
        assertEquals("0", ConfigSynonym.MINUTES_TO_MILLISECONDS.apply("not_a_number"));
    }
}
