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
package org.apache.kafka.clients.consumer.internals;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.Cluster;
import org.junit.Test;

public class PartitionAssignorAdapterTest {

    private List<String> classNames;

    @Test
    public void shouldInstantiateNewAssignors() {
        classNames = Arrays.asList(StickyAssignor.class.getName());
        List<ConsumerPartitionAssignor> assignors = PartitionAssignorAdapter.getAssignorInstances(classNames);
        assertFalse(assignors.isEmpty());
    }

    @Test
    public void shouldAdaptOldAssignors() {
        classNames = Arrays.asList(OldPartitionAssignor.class.getName());
        List<ConsumerPartitionAssignor> assignors = PartitionAssignorAdapter.getAssignorInstances(classNames);
        assertFalse(assignors.isEmpty());
    }

    @Test
    public void shouldThrowOnNonAssignor() {
        classNames = Arrays.asList(NotAnAssignor.class.getName());
        assertThrows(ClassCastException.class, () -> PartitionAssignorAdapter.getAssignorInstances(classNames));
    }

    @SuppressWarnings("deprecation")
    public static class OldPartitionAssignor implements PartitionAssignor {

        @Override
        public Subscription subscription(Set<String> topics) {
            return new Subscription(new ArrayList<>(topics), null);
        }

        @Override
        public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
            return new HashMap<>();
        }

        @Override
        public void onAssignment(Assignment assignment) {
        }

        @Override
        public String name() {
            return "old-assignor";
        }
    }

    public static class NotAnAssignor {

        public NotAnAssignor() {
            throw new IllegalStateException("Should not have been instantiated!");
        }
    }

}
