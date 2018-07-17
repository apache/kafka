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

package org.apache.kafka.castle.common;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.castle.cloud.CastleRemoteCommand;
import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleNode;
import org.apache.kafka.castle.tool.CastleReturnCode;
import org.apache.kafka.trogdor.coordinator.Coordinator;
import org.apache.kafka.trogdor.coordinator.CoordinatorClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Common utility functions for the castle tool.
 */
public final class CastleUtil {
    /**
     * Await the termination of an ExecutorService without the possibility of
     * interruption.
     *
     * @param executor      The ExecutorService
     */
    public final static void awaitTerminationUninterruptibly(ExecutorService executor) {
        boolean wasInterrupted = false;
        while (true) {
            try {
                executor.awaitTermination(36500, TimeUnit.DAYS);
                break;
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }
        }
        if (wasInterrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public final static void deleteFileOrLog(CastleLog castleLog, File file) throws IOException {
        if (file != null) {
            try {
                Files.delete(file.toPath());
            } catch (IOException e) {
                castleLog.printf("*** Unable to delete file %s: %s%n", file.getAbsolutePath(), e);
            }
        }
    }

    public final static void waitFor(int pollIntervalMs, int maxWaitTimeMs, Callable<Boolean> callable)
            throws Exception {
        long startMs = Time.SYSTEM.milliseconds();
        while (true) {
            if (callable.call()) {
                return;
            }
            long currentMs = Time.SYSTEM.milliseconds();
            if (currentMs - startMs > maxWaitTimeMs) {
                throw new RuntimeException("Timed out waiting for " + callable.toString());
            }
            Thread.sleep(pollIntervalMs);
        }
    }

    public static final void killProcess(CastleCluster cluster,
            CastleNode node, String processPattern) throws Exception {
        killProcess(cluster, node, processPattern, "SIGTERM");
    }

    /**
     * Kill any processes which match the provided pattern.
     *
     * @param cluster           The castle cluster.
     * @param node              The castle node.
     * @param processPattern    A pattern used to search for the process
     * @param signalType        The signal type to send.
     * @throws Exception
     */
    public static final void killProcess(CastleCluster cluster,
            CastleNode node, String processPattern, String signalType) throws Exception {
        node.cloud().remoteCommand(node).
            argList(killProcessArgs(processPattern, signalType)).
            mustRun();
    }

    public static List<String> killProcessArgs(String processPattern, String signalType) {
        // We don't want our own process' line to appear in the output.  Therefore, we enclose the
        // first character of the pattern string in brackets.
        processPattern = "[" + processPattern.substring(0, 1) + "]" + processPattern.substring(1);
        List<String> argsList = new ArrayList<>(Arrays.asList(new String[] {
            "-n", "--", "ps", "aux", "|", "awk", "'/" + processPattern +
                "/ { print $2 }'", "|", "xargs", "-r", "kill"
        }));
        argsList.add("-s");
        argsList.add(signalType);
        argsList.add("--");
        return argsList;
    }

    /**
     * Kill any java processes which match the provided pattern.
     *
     * @param cluster           The castle cluster.
     * @param node              The castle node.
     * @param processPattern    A pattern used to search for the process
     * @param force             Use SIGKILL rather than SIGTERM
     * @throws Exception
     */
    public static final void killJavaProcess(CastleCluster cluster,
                                             CastleNode node, String processPattern, boolean force) throws Exception {
        node.cloud().remoteCommand(node).
            argList(killJavaProcessArgs(processPattern, force)).
            mustRun();
    }

    public static List<String> killJavaProcessArgs(String processPattern, boolean force) {
        List<String> argsList = new ArrayList<>(Arrays.asList(new String[] {
            "-n", "--", "jcmd", "|", "awk", "'/" + processPattern +
                "/ { print $1 }'", "|", "xargs", "-r", "kill"
        }));
        if (force) {
            argsList.add("-9");
        }
        argsList.add("--");
        return argsList;
    }

    /**
     * Get the status of a process running on a node.
     *
     * @param cluster           The castle cluster.
     * @param node              The castle node.
     * @param processPattern    A pattern used to search for the process
     * @return                  The role status.
     * @throws Exception
     */
    public static final CastleReturnCode getProcessStatus(CastleCluster cluster,
                                                        CastleNode node, String processPattern) throws Exception {
        String effectivePattern = "[" + processPattern.substring(0, 1) + "]" + processPattern.substring(1);
        StringBuilder stringBuilder = new StringBuilder();
        int retVal = node.cloud().remoteCommand(node).
            captureOutput(stringBuilder).
            args("-n", "--", "ps", "aux", "|", "awk", "'/" + effectivePattern + "/ { print $2 }'").
            run();
        if (retVal != 0) {
            cluster.clusterLog().printf("Unable to determine if %s is running.%n", processPattern);
            return CastleReturnCode.TOOL_FAILED;
        }
        String pidString = stringBuilder.toString().trim();
        if (pidString.isEmpty()) {
            cluster.clusterLog().printf("%s is not running.%n", processPattern);
            return CastleReturnCode.CLUSTER_FAILED;
        }
        cluster.clusterLog().printf("%s is running as pid %s%n", processPattern, pidString);
        return CastleReturnCode.SUCCESS;
    }

    /**
     * Get the status of a java process running on a node.
     *
     * @param cluster           The castle cluster.
     * @param node              The castle node.
     * @param processPattern    A pattern used to search for the process
     * @return                  The role status.
     * @throws Exception
     */
    public static final CastleReturnCode getJavaProcessStatus(CastleCluster cluster,
                                                            CastleNode node, String processPattern) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        int retVal = node.cloud().remoteCommand(node).
            captureOutput(stringBuilder).
            args("-n", "--", "jcmd", "|", "grep", processPattern).
            run();
        if (retVal == 255) {
            cluster.clusterLog().printf("Unable to determine if %s is running.%n", processPattern);
            return CastleReturnCode.TOOL_FAILED;
        } else if (retVal == 1) {
            cluster.clusterLog().printf("%s is not running.%n", processPattern);
            return CastleReturnCode.CLUSTER_FAILED;
        }
        String pidString = stringBuilder.toString();
        int firstSpace = pidString.indexOf(" ");
        if (firstSpace != -1) {
            pidString = pidString.substring(0, firstSpace - 1);
        }
        cluster.clusterLog().printf("%s is running as pid %s%n", processPattern, pidString);
        return CastleReturnCode.SUCCESS;
    }

    public static String[] checkJavaProcessStatusArgs(String processPattern) {
        return new String[] {"-n", "--", "jcmd", "|", "grep", "-q", processPattern};
    }

    /**
     * Create a merged configuration map containing entries from both input maps.
     * Entries from the first map take priority.
     */
    public static Map<String, String> mergeConfig(Map<String, String> map1,
                                                  Map<String, String> map2) {
        HashMap<String, String> results = new HashMap<>(map1);
        for (Map.Entry<String, String> entry : map2.entrySet()) {
            if (!results.containsKey(entry.getKey())) {
                results.put(entry.getKey(), entry.getValue());
            }
        }
        return results;
    }

    public interface CoordinatorFunction<T> {
        T apply(CoordinatorClient client) throws Exception;
    }

    /**
     * Create a coordinator client and open an ssh tunnel, so that we can invoke
     * the Trogdor coordinator.
     */
    public static <T> T invokeCoordinator(final CastleCluster cluster, final CastleNode node,
                                          CoordinatorFunction<T> func) throws Exception {
        try (CastleRemoteCommand.Tunnel tunnel =
                 new CastleRemoteCommand.Tunnel(node, Coordinator.DEFAULT_PORT)) {
            CoordinatorClient coordinatorClient = new CoordinatorClient.Builder().
                maxTries(3).
                target("localhost", tunnel.localPort()).
                log(node.log()).
                build();
            return func.apply(coordinatorClient);
        }
    }
};
