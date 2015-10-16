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
import org.apache.kafka.copycat.runtime.distributed.DistributedHerder;
import org.apache.kafka.copycat.storage.KafkaOffsetBackingStore;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
        Properties connectorProps;

        if (args.length < 2) {
            log.info("Usage: CopycatDistributed worker.properties connector1.properties [connector2.properties ...]");
            System.exit(1);
        }

        String workerPropsFile = args[0];
        workerProps = !workerPropsFile.isEmpty() ? Utils.loadProps(workerPropsFile) : new Properties();

        WorkerConfig workerConfig = new WorkerConfig(workerProps);
        Worker worker = new Worker(workerConfig, new KafkaOffsetBackingStore());
        DistributedHerder herder = new DistributedHerder(worker);
        herder.configure(workerConfig.originals());
        final Copycat copycat = new Copycat(worker, herder);
        copycat.start();

        try {
            for (final String connectorPropsFile : Arrays.copyOfRange(args, 1, args.length)) {
                connectorProps = Utils.loadProps(connectorPropsFile);
                FutureCallback<String> cb = new FutureCallback<>(new Callback<String>() {
                    @Override
                    public void onCompletion(Throwable error, String id) {
                        if (error != null)
                            log.error("Failed to create job for {}", connectorPropsFile);
                    }
                });
                herder.addConnector(Utils.propsToStringMap(connectorProps), cb);
                cb.get();
            }
        } catch (Throwable t) {
            log.error("Stopping after connector error", t);
            copycat.stop();
        }

        // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
        copycat.awaitStop();
    }
}
