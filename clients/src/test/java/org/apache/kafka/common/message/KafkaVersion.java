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
package org.apache.kafka.common.message;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class KafkaVersion implements Comparable<KafkaVersion> {

    private final boolean isHead;
    private final String versionNumber;
    private final boolean isLocal;

    public KafkaVersion(String versionNumber) {
        if (!versionNumber.matches("HEAD|local|([0-9]+(\\.[0-9]+)+)"))
            throw new IllegalArgumentException("Invalid Kafka version: " + versionNumber);
        this.versionNumber = versionNumber;
        this.isHead = "HEAD".equals(versionNumber);
        this.isLocal = "local".equals(versionNumber);
    }

    public static KafkaVersion parse(String version) {
        return new KafkaVersion(version);
    }

    public boolean isHead() {
        return isHead;
    }

    public boolean isLocal() {
        return isLocal;
    }

    public List<Integer> parts() {
        return isHead() || isLocal() ? Collections.emptyList() :
            Arrays.stream(versionNumber.split("\\."))
                .map(Integer::valueOf)
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaVersion that = (KafkaVersion) o;
        return Objects.equals(versionNumber, that.versionNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(versionNumber);
    }

    @Override
    public int compareTo(KafkaVersion o) {
        if (isLocal()) {
            return o.isLocal() ? 0 : 1;
        } else if (isHead()) {
            return o.isLocal() ? -1 :
                    o.isHead() ? 0 : 1;
        } else return o.isLocal() || o.isHead() ? -1 : versionNumber.compareTo(o.versionNumber);
    }

    @Override
    public String toString() {
        return versionNumber;
    }

    public boolean notBefore(String otherVersion) {
        return compareTo(parse(otherVersion)) >= 0;
    }
}
