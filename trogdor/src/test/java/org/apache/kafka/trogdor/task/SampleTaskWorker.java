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

package org.apache.kafka.trogdor.task;

import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.trogdor.common.Platform;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SampleTaskWorker implements TaskWorker {
    private final SampleTaskSpec spec;
    private final ScheduledExecutorService executor;
    private Future<Void> future;
    private WorkerStatusTracker status;

    SampleTaskWorker(SampleTaskSpec spec) {
        this.spec = spec;
        this.executor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("SampleTaskWorker", false));
        this.future = null;
    }

    @Override
    public synchronized void start(Platform platform, WorkerStatusTracker status,
                      final KafkaFutureImpl<String> haltFuture) {
        if (this.future != null)
            return;
        this.status = status;
        this.status.update(new TextNode("active"));

        Long exitMs = spec.nodeToExitMs().get(platform.curNode().name());
        if (exitMs == null) {
            exitMs = Long.MAX_VALUE;
        }
        this.future = platform.scheduler().schedule(executor, () -> {
            haltFuture.complete(spec.error());
            return null;
        }, exitMs);
    }

    @Override
    public void stop(Platform platform) throws Exception {
        this.future.cancel(false);
        this.executor.shutdown();
        this.executor.awaitTermination(1, TimeUnit.DAYS);
        this.status.update(new TextNode("halted"));
    }
}
