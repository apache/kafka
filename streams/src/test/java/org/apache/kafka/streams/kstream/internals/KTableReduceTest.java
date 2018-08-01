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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KTableReduceTest {

    @Test
    public void shouldAddAndSubtract() {
        final AbstractProcessorContext context = new InternalMockProcessorContext();

        final Processor<String, Change<Set<String>>> reduceProcessor =
            new KTableReduce<String, Set<String>>(
                "myStore",
                this::unionNotNullArgs,
                this::differenceNotNullArgs
            ).get();


        final InMemoryKeyValueStore<String, Set<String>> myStore =
            new InMemoryKeyValueStore<>("myStore", null, null);

        context.register(myStore, null);
        reduceProcessor.init(context);
        context.setCurrentNode(new ProcessorNode<>("reduce", reduceProcessor, singleton("myStore")));

        reduceProcessor.process("A", new Change<>(singleton("a"), null));
        assertEquals(singleton("a"), myStore.get("A"));
        reduceProcessor.process("A", new Change<>(singleton("b"), singleton("a")));
        assertEquals(singleton("b"), myStore.get("A"));
        reduceProcessor.process("A", new Change<>(null, singleton("b")));
        assertEquals(emptySet(), myStore.get("A"));
    }

    private Set<String> differenceNotNullArgs(final Set<String> left, final Set<String> right) {
        assertNotNull(left);
        assertNotNull(right);

        final HashSet<String> strings = new HashSet<>(left);
        strings.removeAll(right);
        return strings;
    }

    private Set<String> unionNotNullArgs(final Set<String> left, final Set<String> right) {
        assertNotNull(left);
        assertNotNull(right);

        final HashSet<String> strings = new HashSet<>();
        strings.addAll(left);
        strings.addAll(right);
        return strings;
    }
}
