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

public class AssignorSelector {
    public enum Type { CLIENT, SERVER }

    public AssignorSelector.Type type;
    public String serverAssignor;
    private List<ConsumerGroupHeartbeatRequestData.Assignor> clientAssignors;

    private AssignorSelector(Type type, String serverAssignor,
                             List<ConsumerGroupHeartbeatRequestData.Assignor> clientAssignors) {
        this.type = type;
        if (type == Type.CLIENT) {
            this.clientAssignors = clientAssignors;
        } else if (type == Type.SERVER) {
            this.serverAssignor = serverAssignor;
        }
    }

    public static AssignorSelector newClientAssignors(List<ConsumerGroupHeartbeatRequestData.Assignor> assignors) {
        return new AssignorSelector(Type.CLIENT, null, assignors);
    }

    public static AssignorSelector newServerAssignor(String serverAssignor) {
        return new AssignorSelector(Type.SERVER, serverAssignor, null);
    }
}
