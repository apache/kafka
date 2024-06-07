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
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RegexRouterTest {

    private static String apply(String regex, String replacement, String topic) {
        final Map<String, String> props = new HashMap<>();
        props.put("regex", regex);
        props.put("replacement", replacement);
        final RegexRouter<SinkRecord> router = new RegexRouter<>();
        router.configure(props);
        String sinkTopic = router.apply(new SinkRecord(topic, 0, null, null, null, null, 0)).topic();
        router.close();
        return sinkTopic;
    }

    @Test
    public void staticReplacement() {
        assertEquals("bar", apply("foo", "bar", "foo"));
    }

    @Test
    public void doesntMatch() {
        assertEquals("orig", apply("foo", "bar", "orig"));
    }

    @Test
    public void identity() {
        assertEquals("orig", apply("(.*)", "$1", "orig"));
    }

    @Test
    public void addPrefix() {
        assertEquals("prefix-orig", apply("(.*)", "prefix-$1", "orig"));
    }

    @Test
    public void addSuffix() {
        assertEquals("orig-suffix", apply("(.*)", "$1-suffix", "orig"));
    }

    @Test
    public void slice() {
        assertEquals("index", apply("(.*)-(\\d\\d\\d\\d\\d\\d\\d\\d)", "$1", "index-20160117"));
    }

    @Test
    public void testRegexRouterRetrievesVersionFromAppInfoParser() {
        final RegexRouter<SinkRecord> router = new RegexRouter<>();
        assertEquals(AppInfoParser.getVersion(), router.version());
    }

}
