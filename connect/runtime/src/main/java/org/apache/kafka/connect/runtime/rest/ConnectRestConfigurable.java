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

package org.apache.kafka.connect.runtime.rest;

import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

import javax.ws.rs.core.Configurable;
import javax.ws.rs.core.Configuration;

/**
 * The implementation delegates to {@link ResourceConfig} so that we can handle duplicate
 * registrations deterministically by not re-registering them again.
 */
public class ConnectRestConfigurable implements Configurable<ResourceConfig> {

    private static final Logger log = LoggerFactory.getLogger(ConnectRestConfigurable.class);

    private static final boolean ALLOWED_TO_REGISTER = true;
    private static final boolean NOT_ALLOWED_TO_REGISTER = false;

    private ResourceConfig resourceConfig;

    public ConnectRestConfigurable(ResourceConfig resourceConfig) {
        Objects.requireNonNull(resourceConfig, "ResourceConfig can't be null");
        this.resourceConfig = resourceConfig;
    }


    @Override
    public Configuration getConfiguration() {
        return resourceConfig.getConfiguration();
    }

    @Override
    public ResourceConfig property(String name, Object value) {
        return resourceConfig.property(name, value);
    }

    @Override
    public ResourceConfig register(Object component) {
        if (allowedToRegister(component)) {
            resourceConfig.register(component);
        }
        return resourceConfig;
    }

    @Override
    public ResourceConfig register(Object component, int priority) {
        if (allowedToRegister(component)) {
            resourceConfig.register(component, priority);
        }
        return resourceConfig;
    }

    @Override
    public ResourceConfig register(Object component, Map<Class<?>, Integer> contracts) {
        if (allowedToRegister(component)) {
            resourceConfig.register(component, contracts);
        }
        return resourceConfig;
    }

    @Override
    public ResourceConfig register(Object component, Class... contracts) {
        if (allowedToRegister(component)) {
            resourceConfig.register(component, contracts);
        }
        return resourceConfig;
    }

    @Override
    public ResourceConfig register(Class<?> componentClass, Map<Class<?>, Integer> contracts) {
        if (allowedToRegister(componentClass)) {
            resourceConfig.register(componentClass, contracts);
        }
        return resourceConfig;
    }

    @Override
    public ResourceConfig register(Class<?> componentClass, Class<?>... contracts) {
        if (allowedToRegister(componentClass)) {
            resourceConfig.register(componentClass, contracts);
        }
        return resourceConfig;
    }

    @Override
    public ResourceConfig register(Class<?> componentClass, int priority) {
        if (allowedToRegister(componentClass)) {
            resourceConfig.register(componentClass, priority);
        }
        return resourceConfig;
    }

    @Override
    public ResourceConfig register(Class<?> componentClass) {
        if (allowedToRegister(componentClass)) {
            resourceConfig.register(componentClass);
        }
        return resourceConfig;
    }

    private boolean allowedToRegister(Object component) {
        if (resourceConfig.isRegistered(component)) {
            log.warn("The resource {} is already registered", component);
            return NOT_ALLOWED_TO_REGISTER;
        }
        return ALLOWED_TO_REGISTER;
    }

    private boolean allowedToRegister(Class<?> componentClass) {
        if (resourceConfig.isRegistered(componentClass)) {
            log.warn("The resource {} is already registered", componentClass);
            return NOT_ALLOWED_TO_REGISTER;
        }
        return ALLOWED_TO_REGISTER;
    }
}
