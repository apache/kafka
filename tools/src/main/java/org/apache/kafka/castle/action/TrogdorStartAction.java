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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleNode;
import org.apache.kafka.castle.common.CastleUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.kafka.castle.action.ActionPaths.TROGDOR_START_SCRIPT;

public class TrogdorStartAction extends Action  {
    private final TrogdorDaemonType daemonType;

    public TrogdorStartAction(TrogdorDaemonType daemonType,
            String scope, int initialDelayMs) {
        super(new ActionId(daemonType.startType(), scope),
                new TargetId[]{},
                new String[]{},
                initialDelayMs);
        this.daemonType = daemonType;
    }

    @Override
    public void call(final CastleCluster cluster, final CastleNode node) throws Throwable {
        File configFile = null, log4jFile = null;
        try {
            configFile = writeTrogdorConfig(cluster, node);
            log4jFile = writeTrogdorLog4j(cluster, node);
            CastleUtil.killJavaProcess(cluster, node, daemonType.className(), false);
            node.cloud().remoteCommand(node).args(createSetupPathsCommandLine(daemonType)).mustRun();
            node.cloud().remoteCommand(node).syncTo(configFile.getAbsolutePath(),
                daemonType.propertiesPath()).mustRun();
            node.cloud().remoteCommand(node).syncTo(log4jFile.getAbsolutePath(),
                daemonType.log4jConfPath()).mustRun();
            node.cloud().remoteCommand(node).args(
                runDaemonCommandLine(daemonType, node.nodeName())).mustRun();
        } finally {
            CastleUtil.deleteFileOrLog(node.log(), configFile);
            CastleUtil.deleteFileOrLog(node.log(), log4jFile);
        }
        CastleUtil.waitFor(5, 30000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return 0 == node.cloud().remoteCommand(node).args(
                    CastleUtil.checkJavaProcessStatusArgs(daemonType.className())).run();
            }
        });
    }

    public static String[] createSetupPathsCommandLine(TrogdorDaemonType daemonType) {
        return new String[]{"-n", "--",
            "sudo", "rm", "-rf", daemonType.confDir(), daemonType.logDir(), "&&",
            "sudo", "mkdir", "-p", daemonType.confDir(), daemonType.logDir(), "&&",
            "sudo", "chown", "-R", "`whoami`", daemonType.confDir(), daemonType.logDir()
        };
    }

    public static String[] runDaemonCommandLine(TrogdorDaemonType daemonType, String nodeName) {
        return new String[] {"-n", "--", "nohup", "env",
            String.format("KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"",
                daemonType.log4jConfPath()),
            TROGDOR_START_SCRIPT, daemonType.name(), "--" + daemonType.name() + ".config",
            daemonType.propertiesPath(), "--node-name", nodeName,
            ">" + daemonType.logDir() + "/stdout-stderr.txt", "2>&1", "</dev/null", "&"
        };
    }

    private File writeTrogdorConfig(CastleCluster cluster, CastleNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.env().outputDirectory(),
                String.format("trogdor-%s-%d.conf", daemonType.name(), node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("{%n"));
            osw.write(String.format("  \"platform\": \"org.apache.kafka.trogdor.basic.BasicPlatform\",%n"));
            osw.write(String.format("  \"nodes\": {%n"));
            String prefix = String.format("%n");
            for (Map.Entry<String, CastleNode> entry : cluster.nodes().entrySet()) {
                String nodeName = entry.getKey();
                CastleNode castleNode = entry.getValue();
                osw.write(String.format("%s    \"%s\": {%n", prefix, nodeName));
                prefix = String.format(",%n");
                osw.write(String.format("      \"hostname\": \"%s\",%n",
                    castleNode.privateDns()));
                osw.write(String.format("      \"trogdor.agent.port\": 8888%n"));
                osw.write(String.format("    }"));
            }
            osw.write(String.format("%n"));
            osw.write(String.format("  }%n"));
            osw.write(String.format("}%n"));
            success = true;
            return file;
        } finally {
            Utils.closeQuietly(osw, "temporary trogdor agent file OutputStreamWriter");
            Utils.closeQuietly(fos, "temporary trogdor agent file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    private File writeTrogdorLog4j(CastleCluster cluster, CastleNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.env().outputDirectory(),
                String.format("trogdor-%s-log4j-%d.properties",
                    daemonType.name(), node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("log4j.rootLogger=WARN, kafkaAppender%n"));
            osw.write(String.format("log4j.appender.kafkaAppender=org.apache.log4j.DailyRollingFileAppender%n"));
            osw.write(String.format("log4j.appender.kafkaAppender.DatePattern='.'yyyy-MM-dd-HH%n"));
            osw.write(String.format("log4j.appender.kafkaAppender.File=%s%n",
                daemonType.logPath()));
            osw.write(String.format("log4j.appender.kafkaAppender.layout=org.apache.log4j.PatternLayout%n"));
            osw.write(String.format("log4j.appender.kafkaAppender.layout.ConversionPattern=%s%n%n",
                "[%d] %p %m (%c)%n"));
            osw.write(String.format("log4j.logger.org.apache.kafka=INFO%n"));
            osw.write(String.format("%n"));
            success = true;
            return file;
        } finally {
            Utils.closeQuietly(osw, "temporary broker file OutputStreamWriter");
            Utils.closeQuietly(fos, "temporary broker file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }
};
