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

import org.apache.kafka.jmx.JmxDumpersConfig;
import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleNode;
import org.apache.kafka.castle.common.CastleUtil;
import org.apache.kafka.castle.role.JmxDumperRole;
import org.apache.kafka.castle.tool.CastleTool;

import java.io.File;
import java.io.IOException;

import static org.apache.kafka.castle.action.ActionPaths.JMX_DUMPER_LOGS;
import static org.apache.kafka.castle.action.ActionPaths.JMX_DUMPER_PROPERTIES;
import static org.apache.kafka.castle.action.ActionPaths.JMX_DUMPER_ROOT;
import static org.apache.kafka.castle.action.ActionPaths.KAFKA_RUN_CLASS;

/**
 * Starts the JMXDumper tool.
 */
public final class JmxDumperStartAction extends Action {
    public final static String TYPE = "jmxStart";

    private final JmxDumpersConfig conf;

    public JmxDumperStartAction(String scope, JmxDumperRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[]{},
            new String[] {},
            role.initialDelayMs());
        this.conf = role.conf();
    }

    @Override
    public void call(final CastleCluster cluster, final CastleNode node) throws Throwable {
        File configFile = null;
        try {
            configFile = writeJmxDumperConf(cluster, node);
            CastleUtil.killJavaProcess(cluster, node, JmxDumperRole.CLASS_NAME, true);
            node.cloud().remoteCommand(node).args(createSetupPathsCommandLine()).mustRun();
            node.cloud().remoteCommand(node).syncTo(configFile.getAbsolutePath(),
                JMX_DUMPER_PROPERTIES).mustRun();
            node.cloud().remoteCommand(node).args(createRunDaemonCommandLine()).mustRun();
        } finally {
            CastleUtil.deleteFileOrLog(node.log(), configFile);
        }
    }

    public static String[] createSetupPathsCommandLine() {
        return new String[] {"-n", "--",
            "sudo", "rm", "-rf", JMX_DUMPER_ROOT, JMX_DUMPER_LOGS, "&&",
            "sudo", "mkdir", "-p", JMX_DUMPER_ROOT, JMX_DUMPER_LOGS, "&&",
            "sudo", "chown", "-R", "`whoami`", JMX_DUMPER_ROOT, JMX_DUMPER_LOGS};
    }

    private File writeJmxDumperConf(CastleCluster cluster, CastleNode node) throws IOException {
        File file = new File(cluster.env().outputDirectory(),
                String.format("jmx-dumper-%d.conf", node.nodeIndex()));
        CastleTool.JSON_SERDE.writeValue(file, conf);
        return file;
    }

    public static String[] createRunDaemonCommandLine() {
        return new String[]{"-n", "--", "nohup",
            KAFKA_RUN_CLASS, JmxDumperRole.CLASS_NAME, JMX_DUMPER_PROPERTIES,
            "&>" + JMX_DUMPER_LOGS + "/stdout-stderr.txt", "</dev/null", "&"
        };
    }
}
