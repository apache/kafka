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

import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;

import java.util.List;
import java.util.Objects;

/**
 * Selection of a type of assignor used by a member to get partitions assigned as part of a
 * consumer group. Selection could be one of:
 * <li>CLIENT assignors</li>
 * <li>SERVER assignors</li>
 * <p/>
 * Client assignors include of a list of
 * {@link org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData.Assignor}
 * Server assignors include a name of the server assignor selected, ex. uniform, range.
 */
public class AssignorSelection {
    public enum Type { CLIENT, SERVER }

    private final AssignorSelection.Type type;
    private String serverAssignor;
    private List<ConsumerGroupHeartbeatRequestData.Assignor> clientAssignors;

    private AssignorSelection(Type type, String serverAssignor,
                              List<ConsumerGroupHeartbeatRequestData.Assignor> clientAssignors) {
        this.type = type;
        if (type == Type.CLIENT) {
            this.clientAssignors = clientAssignors;
        } else if (type == Type.SERVER) {
            this.serverAssignor = serverAssignor;
        } else {
            throw new IllegalArgumentException("Unsupported assignor type " + type);
        }
    }

    public static AssignorSelection newClientAssignors(List<ConsumerGroupHeartbeatRequestData.Assignor> assignors) {
        if (assignors == null) {
            throw new IllegalArgumentException("Selected client assignors cannot be null");
        }
        return new AssignorSelection(Type.CLIENT, null, assignors);
    }

    public static AssignorSelection newServerAssignor(String serverAssignor) {
        if (serverAssignor == null) {
            throw new IllegalArgumentException("Selected server assignor name cannot be null");
        }
        if (serverAssignor.isEmpty()) {
            throw new IllegalArgumentException("Selected server assignor name cannot be empty");
        }
        return new AssignorSelection(Type.SERVER, serverAssignor, null);
    }

    public static AssignorSelection defaultAssignor() {
        // TODO: review default selection
        return new AssignorSelection(Type.SERVER, "uniform", null);
    }

    public List<ConsumerGroupHeartbeatRequestData.Assignor> clientAssignors() {
        return this.clientAssignors;
    }

    public String serverAssignor() {
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
                Objects.equals(((AssignorSelection) assignorSelection).clientAssignors,
                        this.clientAssignors) &&
                Objects.equals(((AssignorSelection) assignorSelection).serverAssignor, this.serverAssignor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, clientAssignors, serverAssignor);
    }
}
