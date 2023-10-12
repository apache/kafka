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

package org.apache.kafka.clients.consumer.internals;

import java.util.Objects;
import java.util.Optional;

/**
 * Selection of a type of assignor used by a member to get partitions assigned as part of a
 * consumer group. Currently supported assignors are:
 * <li>SERVER assignors</li>
 * <p/>
 * Server assignors include a name of the server assignor selected, ex. uniform, range.
 */
public class AssignorSelection {
    public enum Type { SERVER }

    private final AssignorSelection.Type type;
    private final Optional<String> serverAssignor;

    private AssignorSelection(Type type, String serverAssignor) {
        this.type = type;
        if (type == Type.SERVER) {
            this.serverAssignor = Optional.ofNullable(serverAssignor);
        } else {
            throw new IllegalArgumentException("Unsupported assignor type " + type);
        }
    }

    public static AssignorSelection newServerAssignor(String serverAssignor) {
        if (serverAssignor == null) {
            throw new IllegalArgumentException("Selected server assignor name cannot be null");
        }
        if (serverAssignor.isEmpty()) {
            throw new IllegalArgumentException("Selected server assignor name cannot be empty");
        }
        return new AssignorSelection(Type.SERVER, serverAssignor);
    }

    public static AssignorSelection defaultAssignor() {
        // TODO: review default selection
        return new AssignorSelection(Type.SERVER, null);
    }

    public Optional<String> serverAssignor() {
        return this.serverAssignor;
    }

    public Type type() {
        return this.type;
    }

    @Override
    public boolean equals(Object assignorSelection) {
        if (this == assignorSelection) return true;
        if (assignorSelection == null || getClass() != assignorSelection.getClass()) return false;
        return Objects.equals(((AssignorSelection) assignorSelection).type, this.type) &&
                Objects.equals(((AssignorSelection) assignorSelection).serverAssignor, this.serverAssignor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, serverAssignor);
    }

    @Override
    public String toString() {
        return "AssignorSelection(" +
                "type=" + type + ", " +
                "serverAssignor='" + serverAssignor + '\'' +
                ')';
    }
}
