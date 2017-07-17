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

package org.apache.kafka.tools;

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * Command class for operations on topics
 */
public class TopicCommand {

    private AdminClient client;

    public static void main(String[] args) throws Exception {

        new TopicCommand().run(args);
    }

    void run(String[] args) throws Exception {

        TopicCommandOptions opts = new TopicCommandOptions(args);

        if (args.length == 0)
            CommandLineUtils.printUsageAndDie(opts.parser, "Create, delete, describe, or change a topic.");

        opts.checkArgs();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.brokerList());

        this.client = AdminClient.create(props);

        try {

            if (opts.has("create")) {
                this.createTopics(opts);
            } else if (opts.has("alter")) {
                // TODO
            } else if (opts.has("list")) {
                this.listTopics(opts);
            } else if (opts.has("describe")) {
                // TODO
            } else if (opts.has("delete")) {
                // TODO
            }

        } catch (ArgumentParserException e) {
            opts.parser.handleError(e);
            Exit.exit(1);
        } catch (Exception e) {
            System.out.println("Error while executing topic command : " + e.getMessage());
            e.printStackTrace();
            Exit.exit(1);
        } finally {
            client.close();
        }
    }

    private void listTopics(TopicCommandOptions opts) throws Exception {

        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(false);

        Collection<TopicListing> collection = this.client.listTopics(options).listings().get();
        for (TopicListing topic : collection) {
            System.out.println(topic.name());
        }
    }

    private void createTopics(TopicCommandOptions opts) throws Exception {

        String topic = opts.topic();
        boolean ifNotExists = opts.ifNotExists();

        if (Topic.hasCollisionChars(topic))
            System.out.println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.");

        try {
            if (opts.has("replicaAssignment")) {

                Map<Integer, List<Integer>> assignment = this.parseReplicaAssignment(opts.replicaAssignment());
                client.createTopics(Collections.singleton(new NewTopic(topic, assignment)));

            } else {

                CommandLineUtils.checkRequiredArgs(opts.parser, opts, Arrays.asList(opts.PARTITIONS, opts.REPLICATION_FACTOR));
                int partitions = opts.partitions();
                short replicas = opts.replicationFactor();

                // TODO : rach aware mode ?

                client.createTopics(Collections.singleton(new NewTopic(topic, partitions, replicas))).all().get();
            }
            System.out.println(String.format("Created topic %s .", topic));
        } catch (ExecutionException e) {

            boolean isTopicExists = e.getCause() instanceof TopicExistsException;
            if ((isTopicExists && !ifNotExists) || !isTopicExists) {
                throw e;
            }
        }
    }

    private Map<Integer, List<Integer>> parseReplicaAssignment(String replicaAssignmentList) throws Exception {

        String[] partitionList = replicaAssignmentList.split(",");
        Map<Integer, List<Integer>> ret = new HashMap<>();
        for (int i = 0; i < partitionList.length; i++) {

            String[] brokerList = partitionList[i].split(":");
            List<Integer> intBrokerList = new ArrayList<>();
            for (String broker: brokerList) {
                intBrokerList.add(Integer.valueOf(broker.trim()));
            }

            List<Integer> duplicateBrokers = Utils.duplicates(intBrokerList);
            if (!duplicateBrokers.isEmpty())
                throw new Exception("Partition replica lists may not contain duplicate entries: " + duplicateBrokers);

            ret.put(i, intBrokerList);
            if (ret.get(i).size() != ret.get(0).size())
                throw new Exception("Partition " + i + " has different replication factor: " + intBrokerList);
        }
        return ret;
    }

    /**
     * Options for TopicCommand
     */
    public static class TopicCommandOptions extends CommandOptions {

        public static final String BROKER_LIST = "brokerList";
        public static final String LIST = "list";
        public static final String CREATE = "create";
        public static final String DELETE = "delete";
        public static final String DESCRIBE = "describe";
        public static final String ALTER = "alter";
        public static final String TOPIC = "topic";
        public static final String PARTITIONS = "partitions";
        public static final String REPLICATION_FACTOR = "replicationFactor";
        public static final String REPLICA_ASSIGNMENT = "replicaAssignment";
        public static final String IF_NOT_EXISTS = "ifNotExists";

        public TopicCommandOptions(String[] args) throws ArgumentParserException {

            super("topic-command", "Create, delete, describe, or change a topic.");

            this.parser.addArgument("--broker-list")
                    .required(true)
                    .type(String.class)
                    .dest(BROKER_LIST)
                    .help("REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.");

            MutuallyExclusiveGroup operationOptions = parser
                    .addMutuallyExclusiveGroup()
                    .required(true)
                    .description("specifying only one in --list, --create, --delete, --alter, --describe");

            operationOptions.addArgument("--list")
                    .action(storeTrue())
                    .dest(LIST)
                    .help("List all available topics.");

            operationOptions.addArgument("--create")
                    .action(storeTrue())
                    .dest(CREATE)
                    .help("Create a new topic.");

            operationOptions.addArgument("--delete")
                    .action(storeTrue())
                    .dest(DELETE)
                    .help("Delete a topic");

            operationOptions.addArgument("--describe")
                    .action(storeTrue())
                    .dest(DESCRIBE)
                    .help("List details for the given topics.");

            operationOptions.addArgument("--alter")
                    .action(storeTrue())
                    .dest(ALTER)
                    .help("Alter the number of partitions, replica assignment, and/or configuration for the topic.");

            this.parser.addArgument("--topic")
                    .dest(TOPIC)
                    .type(String.class)
                    .help("The topic to be create, alter or describe. Can also accept a regular " +
                            "expression except for --create option");

            this.parser.addArgument("--partitions")
                    .dest(PARTITIONS)
                    .type(Integer.class)
                    .help("The number of partitions for the topic being created or \n" +
                            "altered (WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected");

            this.parser.addArgument("--replication-factor")
                    .dest(REPLICATION_FACTOR)
                    .type(Short.class)
                    .help("The replication factor for each partition in the topic being created.");

            this.parser.addArgument("--replica-assignment")
                    .dest(REPLICA_ASSIGNMENT)
                    .type(String.class)
                    .help("A list of manual partition-to-broker assignments for the topic being created or altered. " +
                            "broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
                            "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...");

            this.parser.addArgument("--if-not-exists")
                    .dest(IF_NOT_EXISTS)
                    .action(storeTrue())
                    .type(Boolean.class)
                    .setDefault(false)
                    .help("if set when creating topics, the action will only execute if the topic does not already exist");

            this.ns = this.parser.parseArgs(args);
        }

        public String brokerList() {
            return this.ns.getString(BROKER_LIST);
        }

        public String topic() {
            return this.ns.getString(TOPIC);
        }

        public int partitions() {
            return this.ns.getInt(PARTITIONS);
        }

        public short replicationFactor() {
            return this.ns.getShort(REPLICATION_FACTOR);
        }

        public String replicaAssignment() {
            return this.ns.getString(REPLICA_ASSIGNMENT);
        }

        public boolean ifNotExists() {
            return this.ns.getBoolean(IF_NOT_EXISTS);
        }

        public void checkArgs() throws Exception {

            if (!this.has(LIST) && !this.has(DESCRIBE))
                CommandLineUtils.checkRequiredArgs(this.parser, this, Collections.singletonList(TOPIC));

            // check invalid args
            CommandLineUtils.checkInvalidArgs(this.parser, this, PARTITIONS, Arrays.asList(DESCRIBE, LIST, DELETE));
            CommandLineUtils.checkInvalidArgs(this.parser, this, REPLICATION_FACTOR, Arrays.asList(ALTER, DESCRIBE, LIST, DELETE));
            CommandLineUtils.checkInvalidArgs(this.parser, this, REPLICA_ASSIGNMENT, Arrays.asList(DESCRIBE, LIST, DELETE));
            if (this.has(CREATE))
                CommandLineUtils.checkInvalidArgs(this.parser, this, REPLICA_ASSIGNMENT, Arrays.asList(PARTITIONS, REPLICATION_FACTOR));

            CommandLineUtils.checkInvalidArgs(this.parser, this, IF_NOT_EXISTS, Arrays.asList(ALTER, DESCRIBE, LIST, DELETE));
        }
    }
}
