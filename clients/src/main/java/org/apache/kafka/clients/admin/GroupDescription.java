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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.utils.Utils;

import java.util.List;

/**
 * A detailed description of a single consumer group in the cluster.
 */
public class GroupDescription {

    private final String groupId;
    private final String protocolType;
    private final List<MemberDescription> members;

    public GroupDescription(String groupId, String protocolType, List<MemberDescription> members) {
        this.groupId = groupId;
        this.protocolType = protocolType;
        this.members = members;
    }

    public String groupId() {
        return groupId;
    }

    public String protocolType() {
        return protocolType;
    }

    public List<MemberDescription> members() {
        return members;
    }

    @Override
    public String toString() {
        return "(groupId=" + groupId + ", protocolType=" + protocolType + ", members=" +
            Utils.join(members, ",") + ")";
    }
}
