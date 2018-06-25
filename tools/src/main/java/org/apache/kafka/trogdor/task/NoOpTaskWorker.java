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
import org.apache.kafka.trogdor.common.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoOpTaskWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(NoOpTaskWorker.class);

    private final String id;

    private WorkerStatusTracker status;

    public NoOpTaskWorker(String id) {
        this.id = id;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> errorFuture) throws Exception {
        log.info("{}: Activating NoOpTask.", id);
        this.status = status;
        this.status.update(new TextNode("active"));
    }

    @Override
    public void stop(Platform platform) throws Exception {
        log.info("{}: Deactivating NoOpTask.", id);
        this.status.update(new TextNode("done"));
    }
}
