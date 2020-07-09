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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;

import java.util.Collection;
import java.util.Map;

public class AlterConfigsUtil {

    public static IncrementalAlterConfigsRequestData generateIncrementalRequestData(final Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                                                    final boolean validateOnly) {
        return generateIncrementalRequestData(configs.keySet(), configs, validateOnly);
    }

    public static IncrementalAlterConfigsRequestData generateIncrementalRequestData(final Collection<ConfigResource> resources,
                                                                                    final Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                                                    final boolean validateOnly) {
        IncrementalAlterConfigsRequestData data = new IncrementalAlterConfigsRequestData()
                                                      .setValidateOnly(validateOnly);
        for (ConfigResource resource : resources) {
            IncrementalAlterConfigsRequestData.AlterableConfigCollection alterableConfigSet =
                new IncrementalAlterConfigsRequestData.AlterableConfigCollection();
            for (AlterConfigOp configEntry : configs.get(resource))
                alterableConfigSet.add(new IncrementalAlterConfigsRequestData.AlterableConfig()
                                           .setName(configEntry.configEntry().name())
                                           .setValue(configEntry.configEntry().value())
                                           .setConfigOperation(configEntry.opType().id()));
            IncrementalAlterConfigsRequestData.AlterConfigsResource alterConfigsResource = new IncrementalAlterConfigsRequestData.AlterConfigsResource();
            alterConfigsResource.setResourceType(resource.type().id())
                .setResourceName(resource.name()).setConfigs(alterableConfigSet);
            data.resources().add(alterConfigsResource);
        }
        return data;
    }
}
