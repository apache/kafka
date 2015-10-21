/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

public class TopologyBuilderTest {

    @Test(expected = TopologyException.class)
    public void testAddSourceWithSameName() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addSource("source", "topic-2");
    }

    @Test(expected = TopologyException.class)
    public void testAddSourceWithSameTopic() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addSource("source-2", "topic-1");
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithSameName() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithWrongParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addProcessor("processor", new MockProcessorSupplier(), "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddProcessorWithSelfParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addProcessor("processor", new MockProcessorSupplier(), "processor");
    }

    @Test(expected = TopologyException.class)
    public void testAddSinkWithSameName() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source", "topic-1");
        builder.addSink("sink", "topic-2", "source");
        builder.addSink("sink", "topic-3", "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddSinkWithWrongParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSink("sink", "topic-2", "source");
    }

    @Test(expected = TopologyException.class)
    public void testAddSinkWithSelfParent() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSink("sink", "topic-2", "sink");
    }

    @Test
    public void testSourceTopics() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source-1", "topic-1");
        builder.addSource("source-2", "topic-2");
        builder.addSource("source-3", "topic-3");

        assertEquals(3, builder.sourceTopics().size());
    }

    @Test
    public void testTopicGroups() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source-1", "topic-1", "topic-1x");
        builder.addSource("source-2", "topic-2");
        builder.addSource("source-3", "topic-3");
        builder.addSource("source-4", "topic-4");
        builder.addSource("source-5", "topic-5");

        builder.addProcessor("processor-1", new MockProcessorSupplier(), "source-1");

        builder.addProcessor("processor-2", new MockProcessorSupplier(), "source-2", "processor-1");
        builder.copartitionSources(list("source-1", "source-2"));

        builder.addProcessor("processor-3", new MockProcessorSupplier(), "source-3", "source-4");

        List<List<String>> topicGroups = sort(builder.topicGroups());

        assertEquals(3, topicGroups.size());
        assertEquals(list(list("topic-1", "topic-1x", "topic-2"), list("topic-3", "topic-4"), list("topic-5")), topicGroups);

        List<List<String>> copartitionGroups = sort(builder.copartitionGroups());

        assertEquals(list(list("topic-1", "topic-1x", "topic-2")), copartitionGroups);
    }

    private List<List<String>> sort(Collection<Set<String>> topicGroups) {
        TreeMap<String, String[]> sortedMap = new TreeMap<>();

        for (Set<String> group : topicGroups) {
            String[] arr = group.toArray(new String[group.size()]);
            Arrays.sort(arr);
            sortedMap.put(arr[0], arr);
        }

        ArrayList<List<String>> list = new ArrayList(sortedMap.size());
        for (String[] arr : sortedMap.values()) {
            list.add(Arrays.asList(arr));
        }

        return list;
    }

    private <T> List<T> list(T... elems) {
        List<T> s = new ArrayList<T>();
        for (T elem : elems) {
            s.add(elem);
        }
        return s;
    }

}
