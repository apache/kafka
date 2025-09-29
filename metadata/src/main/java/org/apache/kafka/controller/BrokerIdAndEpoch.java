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

import java.util.Objects;

public class BrokerIdAndEpoch {
    private final int id;
    private final long epoch;

    public BrokerIdAndEpoch(
        int id,
        long epoch
    ) {
        this.id = id;
        this.epoch = epoch;
    }

    public int id() {
        return id;
    }

    public long epoch() {
        return epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!(o instanceof BrokerIdAndEpoch))) return false;
        BrokerIdAndEpoch other = (BrokerIdAndEpoch) o;
        return id == other.id && epoch == other.epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, epoch);
    }

    @Override
    public String toString() {
        return "BrokerIdAndEpoch(id=" + id + ", epoch=" + epoch + ")";
    }
}
