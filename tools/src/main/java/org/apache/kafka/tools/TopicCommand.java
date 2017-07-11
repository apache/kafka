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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.utils.Exit;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicCommand {

    private AdminClient client;

    public static void main(String[] args) throws Exception {

        (new TopicCommand()).run(args);
    }

    void run(String[] args) throws Exception {

        TopicCommandOptions opts = new TopicCommandOptions(args);

        if (args.length == 0)
            CommandLineUtils.printUsageAndDie(opts.parser, "Create, delete, describe, or change a topic.");

        // TODO : should have exactly one action

        opts.checkArgs();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valuesOf(opts.brokerListOpt));

        this.client = AdminClient.create(props);

        try {

            if (opts.options.has(opts.createOpt)) {
                this.createTopics(opts);
            } else if (opts.options.has(opts.alterOpt)) {
                // TODO
            } else if (opts.options.has(opts.listOpt)) {
                this.listTopics(opts);
            } else if (opts.options.has(opts.describeOpt)) {
                // TODO
            } else if (opts.options.has(opts.deleteOpt)) {
                // TODO
            }

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

        String topic = String.valueOf(opts.options.valueOf(opts.topicOpt));
        boolean ifNotExists = opts.options.has(opts.ifNotExistsOpt);

        if (Topic.hasCollisionChars(topic))
            System.out.println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.");

        try {
            if (opts.options.has(opts.replicaAssignmentOpt)) {

                // TODO

            } else {

                CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, Arrays.asList(opts.partitionsOpt, opts.replicationFactorOpt));
                int partitions = (Integer) opts.options.valueOf(opts.partitionsOpt);
                short replicas = (Short) opts.options.valueOf(opts.replicationFactorOpt);

                client.createTopics(Collections.singleton(new NewTopic(topic, partitions, replicas))).all().get();
                System.out.println(String.format("Created topic %s .", topic));
            }
        } catch (ExecutionException e) {

            boolean isTopicExists = e.getCause() instanceof TopicExistsException;
            if ((isTopicExists && !ifNotExists) || !isTopicExists) {
                throw e;
            }
        }
    }

    public class TopicCommandOptions {

        private OptionParser parser;

        public OptionSpec brokerListOpt;
        public OptionSpec listOpt;
        public OptionSpec deleteOpt;
        public OptionSpec alterOpt;
        public OptionSpec describeOpt;
        public OptionSpec createOpt;
        public OptionSpec helpOpt;

        public OptionSpec topicOpt;
        public OptionSpec partitionsOpt;
        public OptionSpec replicationFactorOpt;
        public OptionSpec replicaAssignmentOpt;
        public OptionSpec ifNotExistsOpt;


        public OptionSet options;

        public TopicCommandOptions(String[] args) {
            this.parser = new OptionParser(false);

            this.brokerListOpt = this.parser.accepts("broker-list",
                    "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
                    .withRequiredArg()
                    .describedAs("urls")
                    .ofType(String.class);

            this.listOpt = this.parser.accepts("list", "List all available topics.");
            this.createOpt = parser.accepts("create", "Create a new topic.");
            this.deleteOpt = parser.accepts("delete", "Delete a topic");
            this.alterOpt = parser.accepts("alter", "Alter the number of partitions, replica assignment, and/or configuration for the topic.");
            this.describeOpt = parser.accepts("describe", "List details for the given topics.");
            this.helpOpt = parser.accepts("help", "Print usage information.");

            this.topicOpt = parser.accepts("topic", "The topic to be create, alter or describe. Can also accept a regular " +
                    "expression except for --create option")
                    .withRequiredArg()
                    .describedAs("topic")
                    .ofType(String.class);

            this.partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being created or " +
                    "altered (WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected")
                    .withRequiredArg()
                    .describedAs("# of partitions")
                    .ofType(Integer.class);
            this.replicationFactorOpt = parser.accepts("replication-factor", "The replication factor for each partition in the topic being created.")
                    .withRequiredArg()
                    .describedAs("replication factor")
                    .ofType(Short.class);
            this.replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being created or altered.")
                    .withRequiredArg()
                    .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
                            "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
                    .ofType(String.class);

            this.ifNotExistsOpt = parser.accepts("if-not-exists",
                    "if set when creating topics, the action will only execute if the topic does not already exist");

            this.options = this.parser.parse(args);
        }

        public void checkArgs() throws Exception {

            // check required args
            CommandLineUtils.checkRequiredArgs(parser, options, Collections.singletonList(brokerListOpt));
            if (!options.has(listOpt) && !options.has(describeOpt))
                CommandLineUtils.checkRequiredArgs(parser, options, Collections.singletonList(topicOpt));

            // check invalid args
            CommandLineUtils.checkInvalidArgs(parser, options, partitionsOpt, Arrays.asList(describeOpt, listOpt, deleteOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, replicationFactorOpt, Arrays.asList(alterOpt, describeOpt, listOpt, deleteOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt, Arrays.asList(describeOpt, listOpt, deleteOpt));
            if (options.has(createOpt))
                CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt, Arrays.asList(partitionsOpt, replicationFactorOpt));

            CommandLineUtils.checkInvalidArgs(parser, options, ifNotExistsOpt, Arrays.asList(alterOpt, describeOpt, listOpt, deleteOpt));
        }
    }
}
