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

package org.apache.kafka.trogdor.fault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.trogdor.fault.Kibosh.KiboshFilesUnreadableFaultSpec;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.HashSet;
import java.util.Set;

/**
 * The specification for a fault that makes files unreadable.
 */
public class FilesUnreadableFaultSpec extends TaskSpec {
    private final Set<String> nodeNames;
    private final String mountPath;
    private final String prefix;
    private final int errorCode;

    @JsonCreator
    public FilesUnreadableFaultSpec(@JsonProperty("startMs") long startMs,
                                    @JsonProperty("durationMs") long durationMs,
                                    @JsonProperty("nodeNames") Set<String> nodeNames,
                                    @JsonProperty("mountPath") String mountPath,
                                    @JsonProperty("prefix") String prefix,
                                    @JsonProperty("errorCode") int errorCode) {
        super(startMs, durationMs);
        this.nodeNames = nodeNames == null ? new HashSet<>() : nodeNames;
        this.mountPath = mountPath == null ? "" : mountPath;
        this.prefix = prefix == null ? "" : prefix;
        this.errorCode = errorCode;
    }

    @JsonProperty
    public Set<String> nodeNames() {
        return nodeNames;
    }

    @JsonProperty
    public String mountPath() {
        return mountPath;
    }

    @JsonProperty
    public String prefix() {
        return prefix;
    }

    @JsonProperty
    public int errorCode() {
        return errorCode;
    }

    @Override
    public TaskController newController(String id) {
        return new KiboshFaultController(nodeNames);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new KiboshFaultWorker(id,
            new KiboshFilesUnreadableFaultSpec(prefix, errorCode), mountPath);
    }
}
