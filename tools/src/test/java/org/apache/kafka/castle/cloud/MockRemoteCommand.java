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

import org.apache.kafka.castle.cluster.CastleNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MockRemoteCommand implements RemoteCommand {
    private final Logger log = LoggerFactory.getLogger(MockRemoteCommand.class);

    private final MockCloud cloud;
    private final CastleNode node;
    private Operation operation = Operation.SSH;
    private List<String> args = new ArrayList<>();
    private String local = null;
    private String remote = null;
    private StringBuilder stringBuilder = null;

    MockRemoteCommand(MockCloud cloud, CastleNode node) {
        this.cloud = cloud;
        this.node = node;
    }

    @Override
    public RemoteCommand args(String... args) {
        this.operation = Operation.SSH;
        this.args = Arrays.asList(args);
        this.local = null;
        this.remote = null;
        return this;
    }

    @Override
    public RemoteCommand argList(List<String> args) {
        this.operation = Operation.SSH;
        this.args = Collections.unmodifiableList(new ArrayList<>(args));
        this.local = null;
        this.remote = null;
        return this;
    }

    @Override
    public RemoteCommand syncTo(String local, String remote) {
        this.operation = Operation.RSYNC_TO;
        this.args = null;
        this.local = local;
        this.remote = remote;
        return this;
    }

    @Override
    public RemoteCommand syncFrom(String remote, String local) {
        this.operation = Operation.RSYNC_FROM;
        this.args = null;
        this.local = local;
        this.remote = remote;
        return this;
    }

    @Override
    public RemoteCommand captureOutput(StringBuilder stringBuilder) {
        this.stringBuilder = stringBuilder;
        return this;
    }

    @Override
    public int run() throws Exception {
        return run(commandLine());
    }

    @Override
    public void mustRun() throws Exception {
        List<String> commandLine = commandLine();
        int returnCode = run(commandLine);
        if (returnCode != 0) {
            throw new RemoteCommandResultException(commandLine, returnCode);
        }
    }

    @Override
    public void exec() throws Exception {
        // We don't want to actually exec during unit tests
        mustRun();
    }

    private int run(List<String> commandLine) throws Exception {
        MockCommandResponse response = cloud.removeResponse(node.nodeName(), commandLine);
        if (response == null) {
            log.info("{}: Unexpected command line {}",
                node.nodeName(), CastleRemoteCommand.joinCommandLineArgs(commandLine));
            if (stringBuilder != null) {
                stringBuilder.append("Unexpected command line: ").
                    append(CastleRemoteCommand.joinCommandLineArgs(commandLine));
            }
            return 200;
        } else {
            log.info("Matched command line {} to response", CastleRemoteCommand.joinCommandLineArgs(commandLine));
        }
        String result;
        try {
            result = response.response().get();
        } catch (RemoteCommandResultException exception) {
            return exception.returnCode();
        } finally {
            response.complete().complete(null);
        }
        if (stringBuilder != null) {
            stringBuilder.append(result);
        }
        return 0;
    }

    private List<String> commandLine() {
        switch (operation) {
            case SSH:
                return args;
            case RSYNC_TO:
                return Arrays.asList(rsyncToCommandLine(node, local, remote));
            case RSYNC_FROM:
                return Arrays.asList(rsyncFromCommandLine(node, remote, local));
        }
        throw new RuntimeException("unreachable");
    }

    public static String[] rsyncToCommandLine(CastleNode node, String local, String remote) {
        return new String[] {"rsync", local, node.dns() + ":" + remote};
    }

    public static String[] rsyncFromCommandLine(CastleNode node, String remote, String local) {
        return new String[] {"rsync", node.dns() + ":" + remote, local};
    }
}
