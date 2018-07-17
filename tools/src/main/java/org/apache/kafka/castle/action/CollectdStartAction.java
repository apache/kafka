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
import org.apache.kafka.castle.role.CollectdRole;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import static org.apache.kafka.castle.action.ActionPaths.COLLECTD;
import static org.apache.kafka.castle.action.ActionPaths.COLLECTD_LOGS;
import static org.apache.kafka.castle.action.ActionPaths.COLLECTD_PROPERTIES;
import static org.apache.kafka.castle.action.ActionPaths.COLLECTD_ROOT;

/**
 * Starts the collectd monitoring tool.
 */
public final class CollectdStartAction extends Action {
    public final static String TYPE = "collectdStart";

    public CollectdStartAction(String scope, CollectdRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[]{},
            new String[] {},
            role.initialDelayMs());
    }

    @Override
    public void call(final CastleCluster cluster, final CastleNode node) throws Throwable {

        File configFile = null, log4jFile = null;
        try {
            configFile = writeCollectdConfig(cluster, node);
            CastleUtil.killProcess(cluster, node, COLLECTD, "SIGKILL");
            node.cloud().remoteCommand(node).args(createSetupPathsCommandLine()).mustRun();
            node.cloud().remoteCommand(node).syncTo(configFile.getAbsolutePath(),
                COLLECTD_PROPERTIES).mustRun();
            node.cloud().remoteCommand(node).args(createRunDaemonCommandLine()).mustRun();
        } finally {
            CastleUtil.deleteFileOrLog(node.log(), configFile);
            CastleUtil.deleteFileOrLog(node.log(), log4jFile);
        }
    }

    public static String[] createSetupPathsCommandLine() {
        return new String[] {"-n", "--",
            "sudo", "rm", "-rf", COLLECTD_ROOT, COLLECTD_LOGS, "&&",
            "sudo", "mkdir", "-p", COLLECTD_ROOT, COLLECTD_LOGS, COLLECTD_LOGS + "/csv", "&&",
            "sudo", "chown", "-R", "`whoami`", COLLECTD_ROOT, COLLECTD_LOGS};
    }

    private File writeCollectdConfig(CastleCluster cluster, CastleNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.env().outputDirectory(), String.format("collected-%d.conf",
                node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("Interval 2%n"));
            osw.write(String.format("LoadPlugin logfile%n"));
            osw.write(String.format("<Plugin \"logfile\">%n"));
            osw.write(String.format("   LogLevel \"info\"%n"));
            osw.write(String.format("   File \"%s/collectd.log\"%n", COLLECTD_LOGS));
            osw.write(String.format("   Timestamp true%n"));
            osw.write(String.format("</Plugin>%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("LoadPlugin cpu%n"));
            osw.write(String.format("<Plugin \"cpu\">%n"));
            osw.write(String.format("   ReportByCpu false%n"));
            osw.write(String.format("   ValuesPercentage true%n"));
            osw.write(String.format("</Plugin>%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("LoadPlugin interface%n"));
            osw.write(String.format("<Plugin \"interface\">%n"));
            osw.write(String.format("  Interface \"lo\"%n"));
            osw.write(String.format("  IgnoreSelected true%n"));
            osw.write(String.format("</Plugin>%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("LoadPlugin disk%n"));
            osw.write(String.format("<Plugin \"disk\">%n"));
            osw.write(String.format("  IgnoreSelected true%n"));
            osw.write(String.format("</Plugin>%n"));
            osw.write(String.format("%n"));
            osw.write(String.format("LoadPlugin csv%n"));
            osw.write(String.format("   <Plugin \"csv\">%n"));
            osw.write(String.format("   DataDir \"%s/csv\"%n", COLLECTD_LOGS));
            osw.write(String.format("   StoreRates false%n"));
            osw.write(String.format("</Plugin>%n"));
            success = true;
            return file;
        } finally {
            Utils.closeQuietly(osw, "temporary collectd file OutputStreamWriter");
            Utils.closeQuietly(fos, "temporary collectd file FileOutputStream");
            if (!success) {
                CastleUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    public static String[] createRunDaemonCommandLine() {
        return new String[]{"-n", "--", "nohup",
            COLLECTD, "-f", "-C",  COLLECTD_PROPERTIES, "&>/dev/null", "</dev/null", "&"
        };
    }
}
