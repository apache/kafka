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
import org.apache.kafka.castle.role.CollectdRole;

import static org.apache.kafka.castle.action.ActionPaths.COLLECTD;

/**
 * Stop the collectd monitoring system.
 */
public final class CollectdStopAction extends Action {
    public final static String TYPE = "collectdStop";
    private static final int COLLECTD_STOP_DELAY_MS = 2000;

    public CollectdStopAction(String scope, CollectdRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {},
            role.initialDelayMs());
    }

    @Override
    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        if (node.dns().isEmpty()) {
            node.log().printf("*** Skipping collectdStop, because the node has no DNS address.%n");
            return;
        }
        // Flush the collectd cache
        CastleUtil.killProcess(cluster, node, COLLECTD, "SIGUSR1");
        Thread.sleep(COLLECTD_STOP_DELAY_MS);
        CastleUtil.killProcess(cluster, node, COLLECTD);
    }
}
