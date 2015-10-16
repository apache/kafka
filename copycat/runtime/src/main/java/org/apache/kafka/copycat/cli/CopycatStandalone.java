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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.runtime.Copycat;
import org.apache.kafka.copycat.runtime.Herder;
import org.apache.kafka.copycat.runtime.Worker;
import org.apache.kafka.copycat.runtime.standalone.StandaloneHerder;
import org.apache.kafka.copycat.storage.FileOffsetBackingStore;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
public class CopycatStandalone {
    private static final Logger log = LoggerFactory.getLogger(CopycatStandalone.class);

    public static void main(String[] args) throws Exception {
        Properties workerProps;
        Properties connectorProps;

        if (args.length < 2) {
            log.info("Usage: CopycatStandalone worker.properties connector1.properties [connector2.properties ...]");
            System.exit(1);
        }

        String workerPropsFile = args[0];
        workerProps = !workerPropsFile.isEmpty() ? Utils.loadProps(workerPropsFile) : new Properties();

        WorkerConfig workerConfig = new WorkerConfig(workerProps);
        Worker worker = new Worker(workerConfig, new FileOffsetBackingStore());
        Herder herder = new StandaloneHerder(worker);
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
