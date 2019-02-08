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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.kafka.trogdor.common.JsonUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

public final class Kibosh {
    public static final Kibosh INSTANCE = new Kibosh();

    public final static String KIBOSH_CONTROL = "kibosh_control";

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = KiboshFilesUnreadableFaultSpec.class, name = "unreadable"),
        })
    public static abstract class KiboshFaultSpec {
        @Override
        public final boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return Objects.equals(toString(), o.toString());
        }

        @Override
        public final int hashCode() {
            return toString().hashCode();
        }

        @Override
        public final String toString() {
            return JsonUtil.toJsonString(this);
        }
    }

    public static class KiboshFilesUnreadableFaultSpec extends KiboshFaultSpec {
        private final String prefix;
        private final int errorCode;

        @JsonCreator
        public KiboshFilesUnreadableFaultSpec(@JsonProperty("prefix") String prefix,
                                              @JsonProperty("errorCode") int errorCode) {
            this.prefix = prefix;
            this.errorCode = errorCode;
        }

        @JsonProperty
        public String prefix() {
            return prefix;
        }

        @JsonProperty
        public int errorCode() {
            return errorCode;
        }
    }

    private static class KiboshProcess {
        private final Path controlPath;

        KiboshProcess(String mountPath) {
            this.controlPath = Paths.get(mountPath, KIBOSH_CONTROL);
            if (!Files.exists(controlPath)) {
                throw new RuntimeException("Can't find file " + controlPath);
            }
        }

        synchronized void addFault(KiboshFaultSpec toAdd) throws IOException {
            KiboshControlFile file = KiboshControlFile.read(controlPath);
            List<KiboshFaultSpec> faults = new ArrayList<>(file.faults());
            faults.add(toAdd);
            new KiboshControlFile(faults).write(controlPath);
        }

        synchronized void removeFault(KiboshFaultSpec toRemove) throws IOException {
            KiboshControlFile file = KiboshControlFile.read(controlPath);
            List<KiboshFaultSpec> faults = new ArrayList<>();
            boolean foundToRemove = false;
            for (KiboshFaultSpec fault : file.faults()) {
                if (fault.equals(toRemove)) {
                    foundToRemove = true;
                } else {
                    faults.add(fault);
                }
            }
            if (!foundToRemove) {
                throw new RuntimeException("Failed to find fault " + toRemove + ". ");
            }
            new KiboshControlFile(faults).write(controlPath);
        }
    }

    public static class KiboshControlFile {
        private final List<KiboshFaultSpec> faults;

        public final static KiboshControlFile EMPTY =
            new KiboshControlFile(Collections.<KiboshFaultSpec>emptyList());

        public static KiboshControlFile read(Path controlPath) throws IOException {
            byte[] controlFileBytes = Files.readAllBytes(controlPath);
            return JsonUtil.JSON_SERDE.readValue(controlFileBytes, KiboshControlFile.class);
        }

        @JsonCreator
        public KiboshControlFile(@JsonProperty("faults") List<KiboshFaultSpec> faults) {
            this.faults = faults == null ? new ArrayList<KiboshFaultSpec>() : faults;
        }

        @JsonProperty
        public List<KiboshFaultSpec> faults() {
            return faults;
        }

        public void write(Path controlPath) throws IOException {
            Files.write(controlPath, JsonUtil.JSON_SERDE.writeValueAsBytes(this));
        }

        @Override
        public final boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return Objects.equals(toString(), o.toString());
        }

        @Override
        public final int hashCode() {
            return toString().hashCode();
        }

        @Override
        public final String toString() {
            return JsonUtil.toJsonString(this);
        }
    }

    private final TreeMap<String, KiboshProcess> processes = new TreeMap<>();

    private Kibosh() {
    }

    /**
     * Get or create a KiboshProcess object to manage the Kibosh process at a given path.
     */
    private synchronized KiboshProcess findProcessObject(String mountPath) {
        String path = Paths.get(mountPath).normalize().toString();
        KiboshProcess process = processes.get(path);
        if (process == null) {
            process = new KiboshProcess(mountPath);
            processes.put(path, process);
        }
        return process;
    }

    /**
     * Add a new Kibosh fault.
     */
    void addFault(String mountPath, KiboshFaultSpec spec) throws IOException {
        KiboshProcess process = findProcessObject(mountPath);
        process.addFault(spec);
    }

    /**
     * Remove a Kibosh fault.
     */
    void removeFault(String mountPath, KiboshFaultSpec spec) throws IOException {
        KiboshProcess process = findProcessObject(mountPath);
        process.removeFault(spec);
    }
}
