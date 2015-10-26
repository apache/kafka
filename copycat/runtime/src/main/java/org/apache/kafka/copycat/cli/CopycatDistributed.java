/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.cli;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.runtime.Copycat;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.runtime.distributed.DistributedConfig;
import org.apache.kafka.copycat.runtime.distributed.DistributedHerder;
import org.apache.kafka.copycat.runtime.rest.RestServer;
import org.apache.kafka.copycat.storage.KafkaOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * <p>
 * Command line utility that runs Copycat in distributed mode. In this mode, the process joints a group of other workers
 * and work is distributed among them. This is useful for running Copycat as a service, where connectors can be
 * submitted to the cluster to be automatically executed in a scalable, distributed fashion. This also allows you to
 * easily scale out horizontally, elastically adding or removing capacity simply by starting or stopping worker
 * instances.
 * </p>
 */
@InterfaceStability.Unstable
public class CopycatDistributed {
    private static final Logger log = LoggerFactory.getLogger(CopycatDistributed.class);

    public static void main(String[] args) throws Exception {
        Properties workerProps;

        if (args.length < 1) {
            log.info("Usage: CopycatDistributed worker.properties");
            System.exit(1);
        }

        String workerPropsFile = args[0];
        workerProps = !workerPropsFile.isEmpty() ? Utils.loadProps(workerPropsFile) : new Properties();

        DistributedConfig config = new DistributedConfig(workerProps);
        Worker worker = new Worker(config, new KafkaOffsetBackingStore());
        RestServer rest = new RestServer(config);
        DistributedHerder herder = new DistributedHerder(config, worker, rest.advertisedUrl());
        final Copycat copycat = new Copycat(worker, herder, rest);
        copycat.start();

        // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
        copycat.awaitStop();
    }
}
