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

package org.apache.kafka.streaming.processor.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streaming.processor.KafkaProcessor;
import org.apache.kafka.streaming.processor.KafkaSource;
import org.apache.kafka.streaming.processor.ProcessorContext;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorTopology {

    private Map<String, KafkaProcessor> processors = new HashMap<>();
    private Map<String, KafkaSource> topicSources = new HashMap<>();

    public ProcessorTopology(Map<String, KafkaProcessor> processors, Map<String, KafkaSource> topicSources) {
        this.processors = processors;
        this.topicSources = topicSources;
    }

    public Set<KafkaSource> sources() {
        Set<KafkaSource> sources = new HashSet<>();
        for (KafkaSource source : this.topicSources.values()) {
            sources.add(source);
        }

        return sources;
    }

    public Set<String> topics() {
        return topicSources.keySet();
    }

    public KafkaSource source(String topic) {
        return topicSources.get(topic);
    }

    public Deserializer keyDeser(String topic) {
        KafkaSource source = topicSources.get(topic);

        if (source == null)
            throw new IllegalStateException("The topic " + topic + " is unknown.");

        return source.keyDeserializer;
    }

    public Deserializer valueDeser(String topic) {
        KafkaSource source = topicSources.get(topic);

        if (source == null)
            throw new IllegalStateException("The topic " + topic + " is unknown.");

        return source.valDeserializer;
    }

    /**
     * initialize the processors following the DAG ordering
     * such that parents are always created and initialized before children
     */
    @SuppressWarnings("unchecked")
    public final void init(ProcessorContext context) {
        Deque<KafkaProcessor> deque = new ArrayDeque<>();

        // initialize sources
        for (String topic : topicSources.keySet()) {
            KafkaSource source = topicSources.get(topic);

            source.init(context);
            source.initialized = true;

            // put source children to be traversed first
            for (KafkaProcessor childProcessor : (List<KafkaProcessor>) source.children()) {
                deque.addLast(childProcessor);
            }
        }

        // traverse starting at the sources' children, and initialize the processor
        // if 1) it has not been initialized, 2) all its parents are initialized
        while (!deque.isEmpty()) {
            KafkaProcessor processor = deque.pollFirst();

            if (processor.initialized)
                continue;

            boolean parentsInitialized = true;

            for (KafkaProcessor parentProcessor : (List<KafkaProcessor>) processor.parents()) {
                if (!parentProcessor.initialized) {
                    parentsInitialized = false;
                    break;
                }
            }

            if (parentsInitialized) {
                processor.init(context);
                processor.initialized = true;

                // put source children to be traversed first
                for (KafkaProcessor childProcessor : (List<KafkaProcessor>) processor.children()) {
                    deque.addLast(childProcessor);
                }
            }
        }
    }

    public final void close() {
        // close the processors
        // TODO: do we need to follow the DAG ordering
        for (KafkaProcessor processor : processors.values()) {
            processor.close();
        }

        processors.clear();
        topicSources.clear();
    }
}
