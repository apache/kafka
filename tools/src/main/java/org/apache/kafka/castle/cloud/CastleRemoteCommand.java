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

package org.apache.kafka.castle.cloud;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.castle.cluster.CastleNode;
import org.apache.kafka.castle.common.CastleLog;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class CastleRemoteCommand implements RemoteCommand {
    private static final int OUTPUT_REDIRECTOR_BUFFER_SIZE = 16384;
    private static final String SSH_TUNNEL_ACTIVE = "The ssh tunnel is now active.";

    /**
     * A thread which reads from a pipe and writes the result into a file.
     */
    private static final class OutputRedirector implements Runnable {
        private final InputStream output;
        private final StringBuilder stringBuilder;
        private final CastleLog castleLog;

        OutputRedirector(InputStream output, StringBuilder stringBuilder,
                         CastleLog castleLog) {
            this.output = output;
            this.stringBuilder = stringBuilder;
            this.castleLog = castleLog;
        }

        @Override
        public void run() {
            byte[] arr = new byte[OUTPUT_REDIRECTOR_BUFFER_SIZE];
            try {
                while (true) {
                    int ret = output.read(arr);
                    if (ret == -1) {
                        break;
                    }
                    castleLog.write(arr, 0, ret);
                    if (stringBuilder != null) {
                        stringBuilder.append(new String(arr, StandardCharsets.UTF_8));
                    }
                }
            } catch (EOFException e) {
            } catch (IOException e) {
                castleLog.printf("IOException: %s%n", e.getMessage());
            }
        }
    }

    /**
     * An ssh tunnel that forwards local ports to remote ones.
     */
    public static class Tunnel implements AutoCloseable {
        private final static int MAX_TRIES = 10;
        private final Process process;
        private final int localPort;

        public Tunnel(CastleNode node, int remotePort) throws Exception {
            Process process = null;
            int localPort = -1;
            int tries = 0;
            do {
                localPort = ThreadLocalRandom.current().nextInt(32768, 61000);
                try {
                    process = tryCreateProcess(node, remotePort, localPort);
                } catch (Throwable e) {
                    node.log().info("Unable to create ssh tunnel on local port {}",
                        localPort, e);
                    if (++tries >= MAX_TRIES) {
                        throw e;
                    }
                }
            } while (process == null);
            this.process = process;
            this.localPort = localPort;
        }

        private static Process tryCreateProcess(CastleNode node, int remotePort, int localPort)
                throws Exception {
            Process curProcess = null;
            boolean success = false;
            List<String> commandLine = createSshCommandPreamble(node);
            commandLine.add("-L");
            commandLine.add(String.format("%d:localhost:%d", localPort, remotePort));
            commandLine.add(node.dns());
            commandLine.add("-o");
            commandLine.add("ExitOnForwardFailure=yes");
            commandLine.add("-n");
            commandLine.add("--");
            commandLine.add("echo");
            commandLine.add(SSH_TUNNEL_ACTIVE);
            commandLine.add("&&");
            commandLine.add("sleep");
            commandLine.add("100000");
            node.log().printf("** %s: CREATING SSH TUNNEL: %s%n",
                node.nodeName(), joinCommandLineArgs(commandLine));
            ProcessBuilder builder = new ProcessBuilder(commandLine);
            InputStreamReader isr = null;
            BufferedReader br = null;
            curProcess = builder.start();
            try {
                isr = new InputStreamReader(curProcess.getInputStream(), StandardCharsets.UTF_8);
                br = new BufferedReader(isr);
                String line = br.readLine();
                if ((line == null) || !line.equals(SSH_TUNNEL_ACTIVE)) {
                    throw new RuntimeException("Read unexpected line from ssh tunnel process: " + line);
                }
                success = true;
            } finally {
                Utils.closeQuietly(isr, "tryCreateProcess#InputStreamReader");
                Utils.closeQuietly(br, "tryCreateProcess#BufferedReader");
                if (!success) {
                    curProcess.destroy();
                    curProcess.waitFor();
                }
            }
            node.log().printf("** %s: TUNNEL ESTABLISHED: %s%n",
                node.nodeName(), joinCommandLineArgs(commandLine));
            return curProcess;
        }

        /**
         * Get the local (not remote) port which the ssh tunnel is connected to.
         *
         * @return  The local port.
         */
        public int localPort() {
            return localPort;
        }

        @Override
        public void close() throws InterruptedException {
            process.destroy();
            process.waitFor();
        }
    }

    private Operation operation = Operation.SSH;

    private final CastleNode node;

    private List<String> args = null;

    private String local = null;

    private String remote = null;

    private StringBuilder stringBuilder = null;

    CastleRemoteCommand(CastleNode node) {
        this.node = node;
    }

    @Override
    public CastleRemoteCommand args(String... args) {
        return argList(Arrays.asList(args));
    }

    @Override
    public CastleRemoteCommand argList(List<String> args) {
        this.operation = Operation.SSH;
        this.args = new ArrayList<>(args);
        this.local = null;
        this.remote = null;
        return this;
    }

    @Override
    public CastleRemoteCommand syncTo(String local, String remote) {
        this.operation = Operation.RSYNC_TO;
        this.args = null;
        this.local = local;
        this.remote = remote;
        return this;
    }

    @Override
    public CastleRemoteCommand syncFrom(String remote, String local) {
        this.operation = Operation.RSYNC_FROM;
        this.args = null;
        this.local = local;
        this.remote = remote;
        return this;
    }

    @Override
    public CastleRemoteCommand captureOutput(StringBuilder stringBuilder) {
        this.stringBuilder = stringBuilder;
        return this;
    }

    @Override
    public int run() throws Exception {
        List<String> commandLine = makeCommandLine();
        return run(commandLine);
    }

    @Override
    public void mustRun() throws Exception {
        List<String> commandLine = makeCommandLine();
        int returnCode = run(commandLine);
        if (returnCode != 0) {
            throw new RemoteCommandResultException(commandLine, returnCode);
        }
    }

    @Override
    public void exec() throws Exception {
        List<String> commandLine = makeCommandLine();
        node.log().printf("** %s: SSH %s%n", node.nodeName(), joinCommandLineArgs(commandLine));
        ProcessBuilder builder = new ProcessBuilder(commandLine);
        builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process process = builder.start();
        System.exit(process.waitFor());
    }

    private List<String> makeCommandLine() {
        List<String> commandLine = new ArrayList<>();
        if (node.dns().isEmpty()) {
            throw new RuntimeException("No DNS address configured for " + node.nodeName());
        }
        switch (operation) {
            case SSH:
                if (args == null) {
                    throw new RuntimeException("You must supply ssh arguments.");
                }
                commandLine.addAll(createSshCommandPreamble(node));
                commandLine.add(node.dns());
                commandLine.addAll(args);
                break;
            case RSYNC_TO:
                if ((local == null) || (remote == null)) {
                    throw new RuntimeException("The local and remote paths must be non-null.");
                }
                commandLine.add("rsync");
                commandLine.add("-aqi");
                commandLine.add("--delete");
                commandLine.add("-e");
                commandLine.add(Utils.join(createSshCommandPreamble(node), " "));
                commandLine.add(local);
                commandLine.add(node.dns() + ":" + remote);
                break;
            case RSYNC_FROM:
                if ((local == null) || (remote == null)) {
                    throw new RuntimeException("The local and remote paths must be non-null.");
                }
                commandLine.add("rsync");
                commandLine.add("-aqi");
                commandLine.add("--delete");
                commandLine.add("-e");
                commandLine.add(Utils.join(createSshCommandPreamble(node), " "));
                commandLine.add(node.dns() + ":" + remote);
                commandLine.add(local);
                break;
        }
        return commandLine;
    }

    private int run(List<String> commandLine) throws Exception {
        ProcessBuilder builder = new ProcessBuilder(commandLine);
        builder.redirectErrorStream(true);
        Process process = null;
        Thread outputRedirectorThread = null;
        int retCode;
        try {
            node.log().printf("** %s: RUNNING %s%n", node.nodeName(), joinCommandLineArgs(commandLine));
            process = builder.start();
            OutputRedirector outputRedirector =
                new OutputRedirector(process.getInputStream(), stringBuilder, node.log());
            outputRedirectorThread = new Thread(outputRedirector, "CastleSsh_" + node.nodeName());
            outputRedirectorThread.start();
            retCode = process.waitFor();
            outputRedirectorThread.join();
            node.log().printf(String.format("** %s: FINISHED %s with RESULT %d%n",
                node.nodeName(), joinCommandLineArgs(commandLine), retCode));
        } finally {
            if (process != null) {
                process.destroy();
                process.waitFor();
            }
            if (outputRedirectorThread != null) {
                outputRedirectorThread.join();
            }
        }
        return retCode;
    }

    public static List<String> createSshCommandPreamble(CastleNode node) {
        List<String> commandLine = new ArrayList<>();
        commandLine.add("ssh");
        String identityFile = node.sshIdentityFile();

        // Specify an identity file, if configured.
        if (!identityFile.isEmpty()) {
            commandLine.add("-i");
            commandLine.add(identityFile);
        }
        // Set the user to ssh as, if configured.
        String user = node.sshUser();
        if (!user.isEmpty()) {
            commandLine.add("-l");
            commandLine.add(user);
        }

        // Set the port to ssh to, if configured.
        int port = node.sshPort();
        if (port != 0) {
            commandLine.add("-p");
            commandLine.add(Integer.toString(port));
        }

        // Disable strict host-key checking to avoid getting prompted the first time we connect.
        // TODO: can we enable this on subsequent sshes?
        commandLine.add("-o");
        commandLine.add("StrictHostKeyChecking=no");

        return commandLine;
    }

    /**
     * Translate a list of command-line arguments into a human-readable string.
     * Arguments which contain whitespace will be quoted.
     *
     * @param args  The argument list.
     * @return      A human-readable string.
     */
    public static String joinCommandLineArgs(List<String> args) {
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        for (String arg : args) {
            bld.append(prefix);
            prefix = " ";
            if (arg.contains(" ")) {
                bld.append("\"");
            }
            bld.append(arg);
            if (arg.contains(" ")) {
                bld.append("\"");
            }
        }
        return bld.toString();
    }
}
