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

package org.apache.kafka.soak.tool;

import org.apache.kafka.soak.action.Action;
import org.apache.kafka.soak.action.ActionScheduler;
import org.apache.kafka.soak.action.SshAction;
import org.apache.kafka.soak.cloud.RemoteCommand;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class SoakSsh {
    final static String COMMAND = "ssh";

    static class SoakSshArgs {
        private final List<String> nodeNames;
        private final List<String> command;

        SoakSshArgs(Collection<String> nodeNames, Collection<String> command) {
            this.nodeNames = Collections.unmodifiableList(new ArrayList<>(nodeNames));
            this.command = Collections.unmodifiableList(new ArrayList<>(command));
        }

        public List<String> nodeNames() {
            return nodeNames;
        }

        public List<String> command() {
            return command;
        }
    }

    static SoakSshArgs parse(SoakCluster cluster, Collection<String> targets) {
        List<String> t = new LinkedList<>(targets);
        Iterator<String> iter = t.iterator();
        if (!iter.hasNext()) {
            throw new RuntimeException("Ssh command not found.");
        }
        if (!iter.next().equals(COMMAND)) {
            throw new RuntimeException("Ssh cannot be combined with other actions.");
        }
        iter.remove();
        if (!iter.hasNext()) {
            return new SoakSshArgs(Collections.<String>emptyList(), Collections.<String>emptyList());
        }
        String val = iter.next();
        if (val.equals("all")) {
            iter.remove();
            return new SoakSshArgs(cluster.nodes().keySet(), t);
        }
        ArrayList<String> nodeNames = new ArrayList<>();
        ArrayList<String> command = new ArrayList<>();
        while (true) {
            if (val.equals("--")) {
                iter.remove();
                break;
            }
            if (!cluster.nodes().keySet().contains(val)) {
                iter.remove();
                command.add(val);
                break;
            }
            iter.remove();
            nodeNames.add(val);
            if (!iter.hasNext()) {
                break;
            }
            val = iter.next();
        }
        command.addAll(t);
        return new SoakSshArgs(nodeNames, command);
    }

    public static void run(SoakCluster cluster, List<String> targets) throws Throwable {
        SoakSshArgs args = parse(cluster, targets);
        if (args.nodeNames().isEmpty()) {
            if (args.command().isEmpty()) {
                throw new RuntimeException("You must supply at least one node to ssh to.");
            } else {
                throw new RuntimeException("Unrecognized node: " + args.command.get(0));
            }
        } else if (args.nodeNames().size() == 1) {
            SoakNode node = cluster.nodes().get(args.nodeNames().get(0));
            RemoteCommand command = cluster.cloud().remoteCommand(node).
                argList(args.command());
            command.exec();
        } else {
            if (args.command().isEmpty()) {
                throw new RuntimeException("When sshing to more than one node, you " +
                    "must supply a command.");
            } else {
                sshToMany(cluster, args);
            }
        }
    }

    public static void sshToMany(SoakCluster cluster, SoakSshArgs args) throws Throwable {
        ActionScheduler.Builder builder = new ActionScheduler.Builder(cluster);
        for (String nodeName : args.nodeNames()) {
            Action action = new SshAction(nodeName, args.command());
            builder.addAction(action);
            builder.addTargetName(action.id().toString());
        }
        try (ActionScheduler actionScheduler = builder.build()) {
            actionScheduler.await(cluster.env().timeoutSecs(), TimeUnit.SECONDS);
        }
    }
};
