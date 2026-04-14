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

package org.apache.kafka.connect.openlineage;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A {@link ConnectRestExtension} that emits
 * <a href="https://openlineage.io/">OpenLineage</a> lineage events for every
 * connector lifecycle transition observed in the Connect cluster.
 *
 * <p>To enable, add the following to your Connect worker configuration:
 * <pre>
 *     rest.extension.classes = org.apache.kafka.connect.openlineage.OpenLineageRestExtension
 * </pre>
 *
 * <p>Transport and namespace settings are read from the worker properties
 * (prefixed with {@code openlineage.}) or from the {@code OPENLINEAGE_CONFIG}
 * environment variable (YAML).  See {@link OpenLineageConfig} for details.
 */
public class OpenLineageRestExtension implements ConnectRestExtension {

    private static final Logger log = LoggerFactory.getLogger(OpenLineageRestExtension.class);

    private OpenLineageConfig olConfig;
    private LifecycleMonitor lifecycleMonitor;

    @Override
    public void register(ConnectRestExtensionContext restPluginContext) {
        log.info("Registering OpenLineage extension");
        lifecycleMonitor = new LifecycleMonitor(
            restPluginContext.clusterState(),
            olConfig
        );
        lifecycleMonitor.start();
        log.info("OpenLineage lifecycle monitor started");
    }

    @Override
    public void close() {
        if (lifecycleMonitor != null) {
            lifecycleMonitor.stop();
            log.info("OpenLineage lifecycle monitor stopped");
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        olConfig = new OpenLineageConfig(configs);
        log.info("OpenLineage extension configured with namespace '{}'", olConfig.namespace());
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
