/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.cli;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.runtime.Coordinator;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.runtime.standalone.StandaloneCoordinator;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * <p>
 * Command line utility that runs Copycat as a standalone process. In this mode, work is not
 * distributed. Instead, all the normal Copycat machinery works within a single process. This is
 * useful for ad hoc, small, or experimental jobs.
 * </p>
 * <p>
 * By default, no job configs or offset data is persistent. You can make jobs persistent and
 * fault tolerant by overriding the settings to use file storage for both.
 * </p>
 */
@InterfaceStability.Unstable
public class Copycat {
    private static final Logger log = LoggerFactory.getLogger(Copycat.class);

    public static void main(String[] args) throws Exception {
        CopycatConfig config;
        Properties workerProps;
        Properties connectorProps;

        try {
            config = CopycatConfig.parseCommandLineArgs(args);
        } catch (ConfigException e) {
            log.error(e.getMessage());
            log.info("Usage: copycat [--worker-config worker.properties]"
                    + " [--create-connectors connector1.properties,connector2.properties,...]"
                    + " [--delete-connectors connector1-name,connector2-name,...]");
            System.exit(1);
            return;
        }

        String workerPropsFile = config.getString(CopycatConfig.WORKER_PROPERTIES_CONFIG);
        workerProps = !workerPropsFile.isEmpty() ? Utils.loadProps(workerPropsFile) : new Properties();

        WorkerConfig workerConfig = new WorkerConfig(workerProps);
        Worker worker = new Worker(workerConfig);
        Coordinator coordinator = new StandaloneCoordinator(worker, workerConfig.getUnusedProperties());
        final org.apache.kafka.copycat.runtime.Copycat copycat = new org.apache.kafka.copycat.runtime.Copycat(worker, coordinator);
        copycat.start();

        try {
            // Destroy any requested connectors
            for (final String connName : config.getList(CopycatConfig.DELETE_CONNECTORS_CONFIG)) {
                FutureCallback<Void> cb = new FutureCallback<>(new Callback<Void>() {
                    @Override
                    public void onCompletion(Throwable error, Void result) {
                        if (error != null)
                            log.error("Failed to stop job {}", connName);
                    }
                });
                coordinator.deleteConnector(connName, cb);
                cb.get();
            }

            // Create any new connectors
            for (final String connectorPropsFile : config.getList(CopycatConfig.CREATE_CONNECTORS_CONFIG)) {
                connectorProps = Utils.loadProps(connectorPropsFile);
                FutureCallback<String> cb = new FutureCallback<>(new Callback<String>() {
                    @Override
                    public void onCompletion(Throwable error, String id) {
                        if (error != null)
                            log.error("Failed to create job for {}", connectorPropsFile);
                    }
                });
                coordinator.addConnector(connectorProps, cb);
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
