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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.controller.QuorumController.Builder;
import org.apache.kafka.metalog.LocalLogManagerTestEnv;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class QuorumControllerTestEnv implements AutoCloseable {
    private static final Logger log =
        LoggerFactory.getLogger(QuorumControllerTestEnv.class);

    private final List<QuorumController> controllers;
    private final LocalLogManagerTestEnv logEnv;

    public QuorumControllerTestEnv(
        LocalLogManagerTestEnv logEnv,
        Consumer<QuorumController.Builder> builderConsumer
    ) throws Exception {
        this(logEnv, builderConsumer, OptionalLong.empty(), OptionalLong.empty());
    }

    public QuorumControllerTestEnv(
        LocalLogManagerTestEnv logEnv,
        Consumer<Builder> builderConsumer,
        OptionalLong sessionTimeoutMillis,
        OptionalLong leaderImbalanceCheckIntervalNs
    ) throws Exception {
        this.logEnv = logEnv;
        int numControllers = logEnv.logManagers().size();
        this.controllers = new ArrayList<>(numControllers);
        try {
            for (int i = 0; i < numControllers; i++) {
                QuorumController.Builder builder = new QuorumController.Builder(i, logEnv.clusterId());
                builder.setRaftClient(logEnv.logManagers().get(i));
                sessionTimeoutMillis.ifPresent(timeout -> {
                    builder.setSessionTimeoutNs(NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS));
                });
                builder.setLeaderImbalanceCheckIntervalNs(leaderImbalanceCheckIntervalNs);
                builderConsumer.accept(builder);
                this.controllers.add(builder.build());
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    QuorumController activeController() throws InterruptedException {
        AtomicReference<QuorumController> value = new AtomicReference<>(null);
        TestUtils.retryOnExceptionWithTimeout(20000, 3, () -> {
            LeaderAndEpoch leader = logEnv.leaderAndEpoch();
            for (QuorumController controller : controllers) {
                if (OptionalInt.of(controller.nodeId()).equals(leader.leaderId()) &&
                    controller.curClaimEpoch() == leader.epoch()) {
                    value.set(controller);
                    break;
                }
            }

            if (value.get() == null) {
                throw new RuntimeException(String.format("Expected to see %s as leader", leader));
            }
        });

        return value.get();
    }

    public List<QuorumController> controllers() {
        return controllers;
    }

    @Override
    public void close() throws InterruptedException {
        for (QuorumController controller : controllers) {
            controller.beginShutdown();
        }
        for (QuorumController controller : controllers) {
            controller.close();
        }
    }
}
