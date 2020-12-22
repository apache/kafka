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

package org.apache.kafka.tools.metadata;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metalog.MetaLogLeader;
import org.apache.kafka.metalog.MetaLogListener;
import org.apache.kafka.tools.metadata.MetadataNode.DirectoryNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Maintains the in-memory metadata for the metadata tool.
 */
public final class MetadataNodeManager implements AutoCloseable, MetaLogListener {
    private static final Logger log = LoggerFactory.getLogger(MetadataNodeManager.class);

    public static class Data {
        private final DirectoryNode root = new DirectoryNode(null);
        private String workingDirectory = "/";

        public DirectoryNode root() {
            return root;
        }

        public String workingDirectory() {
            return workingDirectory;
        }

        public void setWorkingDirectory(String workingDirectory) {
            this.workingDirectory = workingDirectory;
        }
    }

    private final Data data = new Data();
    private final KafkaEventQueue queue;

    public MetadataNodeManager() {
        this.queue = new KafkaEventQueue(Time.SYSTEM,
            new LogContext("[node-manager-event-queue] "), "");
    }

    @Override
    public void close() throws Exception {
        queue.close();
    }

    @Override
    public void handleCommits(long lastOffset, List<ApiMessage> messages) {
        appendEvent("handleCommits", () -> {
            DirectoryNode dir = data.root.mkdirs("@metadata");
            dir.create("offset").setContents(String.valueOf(lastOffset));
        }, null);
    }

    @Override
    public void handleNewLeader(MetaLogLeader leader) {
        appendEvent("handleNewLeader", () -> {
            DirectoryNode dir = data.root.mkdirs("@metadata");
            dir.create("leader").
                setContents(leader.toString());
        }, null);
    }

    public void visit(Consumer<Data> consumer) throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        appendEvent("visit", () -> {
            consumer.accept(data);
            future.complete(null);
        }, future);
        future.get();
    }

    private void appendEvent(String name, Runnable runnable, CompletableFuture<?> future) {
        queue.append(new EventQueue.Event() {
            @Override
            public void run() throws Exception {
                runnable.run();
            }

            @Override
            public void handleException(Throwable e) {
                log.error("Unexpected error while handling event " + name, e);
                if (future != null) {
                    future.completeExceptionally(e);
                }
            }
        });
    }
}
