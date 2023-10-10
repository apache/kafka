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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

public class Replica {

    final int brokerId;
    final Uuid directory;

    public Replica(int brokerId, Uuid directory) {
        this.brokerId = brokerId;
        this.directory = directory;
    }

    public int brokerId() {
        return brokerId;
    }

    public Uuid directory() {
        return directory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Replica that = (Replica) o;
        return brokerId == that.brokerId && Objects.equals(directory, that.directory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, directory);
    }

    @Override
    public String toString() {
        return String.format("%d:%s", brokerId, directory);
    }
}
