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

package org.apache.kafka.castle.action;

import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleNode;
import org.apache.kafka.castle.common.CastleUtil;
import org.apache.kafka.castle.role.BrokerRole;

/**
 * Stop the broker.
 */
public final class BrokerStopAction extends Action {
    public final static String TYPE = "brokerStop";

    public BrokerStopAction(String scope, BrokerRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(JmxDumperStopAction.TYPE, scope)
            },
            new String[] {},
            role.initialDelayMs());
    }

    @Override
    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        if (node.dns().isEmpty()) {
            node.log().printf("*** Skipping brokerStop, because the node has no DNS address.%n");
            return;
        }
        CastleUtil.killJavaProcess(cluster, node, BrokerRole.KAFKA_CLASS_NAME, true);
    }
}
