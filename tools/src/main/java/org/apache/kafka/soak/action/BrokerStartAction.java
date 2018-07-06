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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakUtil;
import org.apache.kafka.soak.role.BrokerRole;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import static org.apache.kafka.soak.action.ActionPaths.KAFKA_CONF;
import static org.apache.kafka.soak.action.ActionPaths.KAFKA_OPLOGS;
import static org.apache.kafka.soak.role.BrokerRole.KAFKA_CLASS_NAME;
import static org.apache.kafka.soak.action.ActionPaths.KAFKA_LOGS;

/**
 * Starts the Kafka broker.
 */
public final class BrokerStartAction extends Action {
    public final static String TYPE = "brokerStart";

    private static final String DEFAULT_JVM_PERFORMANCE_OPTS = "-Xmx3g -Xms3g";

    private final Map<String, String> conf;

    private final String jvmOptions;

    public BrokerStartAction(String scope, Map<String, String> conf, String jvmOptions) {
        super(new ActionId(TYPE, scope),
            new TargetId[]{
                new TargetId(ZooKeeperStartAction.TYPE)
            },
            new String[] {});
        this.conf = Objects.requireNonNull(conf);
        this.jvmOptions = jvmOptions.isEmpty() ? DEFAULT_JVM_PERFORMANCE_OPTS :
            Objects.requireNonNull(jvmOptions);
    }

    @Override
    public void call(final SoakCluster cluster, final SoakNode node) throws Throwable {
        File configFile = null, log4jFile = null;
        try {
            configFile = writeBrokerConfig(cluster, node);
            log4jFile = writeBrokerLog4j(cluster, node);
            SoakUtil.killJavaProcess(cluster, node, KAFKA_CLASS_NAME, true);
            cluster.cloud().remoteCommand(node).args(createSetupPathsCommandLine()).mustRun();
            cluster.cloud().remoteCommand(node).syncTo(configFile.getAbsolutePath(),
                ActionPaths.KAFKA_BROKER_PROPERTIES).mustRun();
            cluster.cloud().remoteCommand(node).syncTo(log4jFile.getAbsolutePath(),
                ActionPaths.KAFKA_BROKER_LOG4J).mustRun();
            cluster.cloud().remoteCommand(node).args(createRunDaemonCommandLine()).mustRun();
        } finally {
            SoakUtil.deleteFileOrLog(node.log(), configFile);
            SoakUtil.deleteFileOrLog(node.log(), log4jFile);
        }
        SoakUtil.waitFor(5, 30000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return 0 == cluster.cloud().remoteCommand(node).args(
                    SoakUtil.checkJavaProcessStatusArgs(KAFKA_CLASS_NAME)).run();
            }
        });
    }

    public static String[] createSetupPathsCommandLine() {
        return new String[] {"-n", "--",
            "sudo", "rm", "-rf", KAFKA_OPLOGS, KAFKA_LOGS, KAFKA_CONF, "&&",
            "sudo", "mkdir", "-p", KAFKA_OPLOGS, KAFKA_LOGS, KAFKA_CONF, "&&",
            "sudo", "chown", "-R", "`whoami`", KAFKA_OPLOGS, KAFKA_LOGS, KAFKA_CONF};
    }

    public String[] createRunDaemonCommandLine() {
        return new String[]{"-n", "--", "nohup", "env",
            "JMX_PORT=9192",
            "KAFKA_JVM_PERFORMANCE_OPTS='" + jvmOptions + "'",
            "KAFKA_LOG4J_OPTS='-Dlog4j.configuration=file:" + ActionPaths.KAFKA_BROKER_LOG4J + "' ",
            ActionPaths.KAFKA_START_SCRIPT, ActionPaths.KAFKA_BROKER_PROPERTIES,
            ">" + ActionPaths.KAFKA_LOGS + "/stdout-stderr.txt", "2>&1", "</dev/null", "&"
        };
    }

    private Map<String, String> getDefaultConf() {
        Map<String, String> defaultConf;
        defaultConf = new HashMap<>();
        defaultConf.put("num.network.threads", "3");
        defaultConf.put("num.io.threads", "8");
        defaultConf.put("socket.send.buffer.bytes", "102400");
        defaultConf.put("socket.receive.buffer.bytes", "102400");
        defaultConf.put("socket.receive.buffer.bytes", "102400");
        defaultConf.put("socket.request.max.bytes", "104857600");
        defaultConf.put("num.partitions", "3");
        defaultConf.put("num.recovery.threads.per.data.dir", "1");
        defaultConf.put("log.retention.bytes", "104857600");
        defaultConf.put("zookeeper.connection.timeout.ms", "6000");
        return defaultConf;
    }

    private File writeBrokerConfig(SoakCluster cluster, SoakNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        Map<String, String> effectiveConf = SoakUtil.mergeConfig(conf, getDefaultConf());
        try {
            file = new File(cluster.env().outputDirectory(), String.format("broker-%d.properties",
                node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("broker.id=%d%n", getBrokerId(cluster, node)));
            osw.write(String.format("listeners=PLAINTEXT://:9092%n"));
            osw.write(String.format("advertised.host.name=%s%n",
                cluster.nodes().get(node.nodeName()).spec().privateDns()));
            osw.write(String.format("log.dirs=%s%n", KAFKA_OPLOGS));
            osw.write(String.format("zookeeper.connect=%s%n", cluster.getZooKeeperConnectString()));
            for (Map.Entry<String, String> entry : effectiveConf.entrySet()) {
                osw.write(String.format("%s=%s%n", entry.getKey(), entry.getValue()));
            }
            success = true;
            return file;
        } finally {
            Utils.closeQuietly(osw, "temporary broker file OutputStreamWriter");
            Utils.closeQuietly(fos, "temporary broker file FileOutputStream");
            if (!success) {
                SoakUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    File writeBrokerLog4j(SoakCluster cluster,  SoakNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.env().outputDirectory(), String.format("broker-log4j-%d.properties",
                node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("log4j.rootLogger=INFO, kafkaAppender%n"));
            osw.write(String.format("%n"));
            writeDailyRollingFileAppender(osw, "kafkaAppender", "server.log");
            writeDailyRollingFileAppender(osw, "stateChangeAppender", "state-change.log");
            writeDailyRollingFileAppender(osw, "requestAppender", "kafka-request.log");
            writeDailyRollingFileAppender(osw, "cleanerAppender", "log-cleaner.log");
            writeDailyRollingFileAppender(osw, "controllerAppender", "controller.log");
            writeDailyRollingFileAppender(osw, "authorizerAppender", "kafka-authorizer.log");
            osw.write(String.format("log4j.logger.org.I0Itec.zkclient.ZkClient=INFO%n"));
            osw.write(String.format("log4j.logger.org.apache.zookeeper=INFO%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.kafka=INFO%n"));
            osw.write(String.format("log4j.logger.org.apache.kafka=INFO%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.kafka.request.logger=WARN, requestAppender%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.kafka.controller=TRACE, controllerAppender%n"));
            osw.write(String.format("log4j.additivity.kafka.controller=false%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.kafka.log.LogCleaner=INFO, cleanerAppender%n"));
            osw.write(String.format("log4j.additivity.kafka.log.LogCleaner=false%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.state.change.logger=TRACE, stateChangeAppender%n"));
            osw.write(String.format("log4j.additivity.state.change.logger=false%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("log4j.logger.kafka.authorizer.logger=INFO, authorizerAppender%n"));
            osw.write(String.format("log4j.additivity.kafka.authorizer.logger=false%n"));
            success = true;
            return file;
        } finally {
            Utils.closeQuietly(osw, "temporary broker log4j file OutputStreamWriter");
            Utils.closeQuietly(fos, "temporary broker log4j file FileOutputStream");
            if (!success) {
                SoakUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    static void writeDailyRollingFileAppender(OutputStreamWriter osw, String appender,
                                              String logName) throws IOException {
        osw.write(String.format("log4j.appender.%s=org.apache.log4j.DailyRollingFileAppender%n", appender));
        osw.write(String.format("log4j.appender.%s.DatePattern='.'yyyy-MM-dd-HH%n", appender));
        osw.write(String.format("log4j.appender.%s.File=%s/%s%n", appender, KAFKA_LOGS, logName));
        osw.write(String.format("log4j.appender.%s.layout=org.apache.log4j.PatternLayout%n", appender));
        osw.write(String.format("log4j.appender.%s.layout.ConversionPattern=%s%n",
            appender, "[%d] %p %m (%c)%n"));
    }

    private int getBrokerId(SoakCluster cluster, SoakNode node) {
        for (Map.Entry<Integer, String> entry :
                cluster.nodesWithRole(BrokerRole.class).entrySet()) {
            if (entry.getValue().equals(node.nodeName())) {
                return entry.getKey();
            }
        }
        throw new RuntimeException("Node " + node.nodeName() + " does not have the broker role.");
    }

}
