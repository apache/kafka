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

package org.apache.kafka.soak.action;

import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakUtil;

/**
 * Stop Trogdor.
 */
public final class TrogdorStopAction extends Action {
    private final TrogdorDaemonType daemonType;

    public TrogdorStopAction(TrogdorDaemonType daemonType, String nodeName, int initialDelayMs) {
        super(new ActionId(daemonType.stopType(), nodeName),
            new TargetId[] {},
            new String[] {},
            initialDelayMs);
        this.daemonType = daemonType;
    }

    @Override
    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
        if (node.dns().isEmpty()) {
            node.log().printf("*** Skipping trogdorStop, because the node has no DNS address.%n");
            return;
        }
        SoakUtil.killJavaProcess(cluster, node, daemonType.className(), false);
    }
}
