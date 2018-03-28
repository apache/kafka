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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.soak.tool.SoakReturnCode.CLUSTER_FAILED;

/**
 * Running ssh.
 */
public final class SshAction extends Action {
    public final static String TYPE = "ssh";

    private final List<String> command;

    public SshAction(String scope, Collection<String> command) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {});
        this.command = Collections.unmodifiableList(new ArrayList<>(command));
    }

    @Override
    public void call(final SoakCluster cluster, final SoakNode node) throws Throwable {
        int status = cluster.cloud().remoteCommand(node).argList(command).run();
        if (status != 0) {
            cluster.shutdownManager().changeReturnCode(CLUSTER_FAILED);
        }
    }
}
