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
import org.apache.kafka.castle.role.UbuntuNodeRole;

/**
 * Install some necessary components on Ubuntu.
 */
public final class UbuntuSetupAction extends Action {
    public final static String TYPE = "ubuntuSetup";

    public UbuntuSetupAction(String scope, UbuntuNodeRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {},
            role.initialDelayMs());
    }

    @Override
    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        node.log().printf("*** %s: Beginning UbuntuSetup...%n", node.nodeName());
        cluster.cloud().remoteCommand(node).args("-n", "--",
            "sudo", "dpkg", "--configure", "-a", "&&",
            "sudo", "apt-get", "update", "-y", "&&",
            "sudo", "apt-get", "install", "-y", "iptables", "rsync", "wget", "curl", "collectd-core",
            "coreutils", "cmake", "pkg-config", "libfuse-dev", "openjdk-8-jdk-headless").mustRun();
        node.log().printf("*** %s: Finished UbuntuSetup.%n", node.nodeName());
    }
};
