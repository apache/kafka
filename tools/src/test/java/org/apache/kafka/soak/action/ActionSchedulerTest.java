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

package org.apache.kafka.soak.action;

import org.apache.kafka.soak.cluster.MiniSoakCluster;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ActionSchedulerTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testCreateDestroy() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder =
            new MiniSoakCluster.Builder().addNodes("node0", "node1");
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(1000, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Test
    public void testInvalidTargetName() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder =
            new MiniSoakCluster.Builder().addNodes("node0", "node1");
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            schedulerBuilder.addTargetName("unknownTarget");
            try {
                try (ActionScheduler scheduler = schedulerBuilder.build()) {
                    scheduler.await(1, TimeUnit.DAYS);
                }
                fail("Expected to get an exception about an unknown target.");
            } catch (RuntimeException e) {
                assertTrue(e.getMessage().contains("Unknown target"));
            }
        }
    }

    @Test
    public void testRunActions() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder =
            new MiniSoakCluster.Builder().addNodes("node0", "node1", "node2");
        final CyclicBarrier barrier = new CyclicBarrier(3);
        final AtomicInteger numRun = new AtomicInteger(0);
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            for (final String nodeName : miniCluster.cluster().nodes().keySet()) {
                schedulerBuilder.addAction(new Action(
                    new ActionId("testAction", nodeName),
                        new TargetId[0],
                        new String[0],
                        0) {
                    @Override
                    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                        numRun.incrementAndGet();
                        barrier.await();
                    }
                });
            }
            schedulerBuilder.addTargetName("testAction");
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(1, TimeUnit.DAYS);
            }
        }
        assertEquals(3, numRun.get());
    }

    @Test
    public void testContainedDependencies() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder =
            new MiniSoakCluster.Builder().addNodes("node0", "node1", "node2");
        Map<String, AtomicInteger> vals = Collections.synchronizedMap(new HashMap<>());
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            for (final String nodeName : miniCluster.cluster().nodes().keySet()) {
                vals.put(nodeName, new AtomicInteger(0));
            }
            vals.put("node0", new AtomicInteger(-1));
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            schedulerBuilder.addAction(new Action(
                    new ActionId("foo", "node0"),
                    new TargetId[0],
                    new String[0],
                    0) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    AtomicInteger val = vals.get(node.nodeName());
                    assertTrue(val.compareAndSet(-1, 0));
                }
            });
            for (final String nodeName : miniCluster.cluster().nodes().keySet()) {
                schedulerBuilder.addAction(new Action(
                    new ActionId("bar", nodeName),
                    new TargetId[] {
                        new TargetId("foo")
                    },
                    new String[] {
                        "baz",
                        "quux"
                    },
                    0) {
                    @Override
                    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                        AtomicInteger val = vals.get(node.nodeName());
                        assertTrue(val.compareAndSet(0, 1));
                    }
                });
                schedulerBuilder.addAction(new Action(
                    new ActionId("baz", nodeName),
                    new TargetId[] {},
                    new String[] {},
                    0) {
                    @Override
                    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                        AtomicInteger val = vals.get(node.nodeName());
                        assertTrue(val.compareAndSet(1, 2));
                    }
                });
                schedulerBuilder.addAction(new Action(
                    new ActionId("quux", nodeName),
                    new TargetId[] {
                        new TargetId("baz", nodeName)
                    },
                    new String[] {},
                    0) {
                    @Override
                    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                        AtomicInteger val = vals.get(node.nodeName());
                        assertTrue(val.compareAndSet(2, 3));
                    }
                });
            }
            schedulerBuilder.addTargetName("foo");
            schedulerBuilder.addTargetName("bar");
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(1000, TimeUnit.MILLISECONDS);
            }
            for (final String nodeName : miniCluster.cluster().nodes().keySet()) {
                assertEquals(3, vals.get(nodeName).get());
            }
        }
    }

    @Test
    public void testAllDependency() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder =
            new MiniSoakCluster.Builder().addNodes("node0", "node1", "node2");
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            final AtomicInteger count = new AtomicInteger(0);
            final AtomicInteger numBars = new AtomicInteger(0);
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            schedulerBuilder.addAction(new Action(
                new ActionId("baz", "node1"),
                new TargetId[0],
                new String[0],
                0) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    count.incrementAndGet();
                }
            });
            for (final String nodeName : miniCluster.cluster().nodes().keySet()) {
                schedulerBuilder.addAction(new Action(
                    new ActionId("foo", nodeName),
                    new TargetId[0],
                    new String[] {
                        "bar",
                        "quux"
                    },
                    0) {
                    @Override
                    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                        count.incrementAndGet();
                    }
                });
                schedulerBuilder.addAction(new Action(
                    new ActionId("quux", nodeName),
                    new TargetId[0],
                    new String[] {},
                    0) {
                    @Override
                    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                        count.incrementAndGet();
                    }
                });
                schedulerBuilder.addAction(new Action(
                    new ActionId("bar", nodeName),
                    new TargetId[]{
                        new TargetId("quux"),
                        new TargetId("baz", "node1")
                    },
                    new String[0],
                    0) {
                    @Override
                    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                        assertEquals(7, count.get());
                        numBars.incrementAndGet();
                    }
                });
            }
            schedulerBuilder.addTargetName("foo");
            schedulerBuilder.addTargetName("baz:node1");
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(1000, TimeUnit.MILLISECONDS);
            }
            assertEquals(3, numBars.get());
        }
    }
};
