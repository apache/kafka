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
package org.apache.kafka.connect.runtime.isolation;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Collection;

public class ModuleScanResult {
    private final Collection<ModuleDesc<Connector>> connectors;
    private final Collection<ModuleDesc<Converter>> converters;
    private final Collection<ModuleDesc<Transformation>> transformations;

    public ModuleScanResult(
            Collection<ModuleDesc<Connector>> connectors,
            Collection<ModuleDesc<Converter>> converters,
            Collection<ModuleDesc<Transformation>> transformations
    ) {
        this.connectors = connectors;
        this.converters = converters;
        this.transformations = transformations;
    }

    public Collection<ModuleDesc<Connector>> connectors() {
        return connectors;
    }

    public Collection<ModuleDesc<Converter>> converters() {
        return converters;
    }

    public Collection<ModuleDesc<Transformation>> transformations() {
        return transformations;
    }

    public boolean isEmpty() {
        return connectors().isEmpty() && converters().isEmpty() && transformations().isEmpty();
    }
}
