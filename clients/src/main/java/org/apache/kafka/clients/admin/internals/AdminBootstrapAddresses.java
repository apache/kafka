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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

final public class AdminBootstrapAddresses {
    private final boolean usingBootstrapControllers;
    private final List<InetSocketAddress> addresses;

    AdminBootstrapAddresses(
        boolean usingBootstrapControllers,
        List<InetSocketAddress> addresses
    ) {
        this.usingBootstrapControllers = usingBootstrapControllers;
        this.addresses = addresses;
    }

    public boolean usingBootstrapControllers() {
        return usingBootstrapControllers;
    }

    public List<InetSocketAddress> addresses() {
        return addresses;
    }

    public static AdminBootstrapAddresses fromConfig(AbstractConfig config) {
        List<String> bootstrapServers = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrapServers == null) {
            bootstrapServers = Collections.emptyList();
        }
        List<String> controllerServers = config.getList(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG);
        if (controllerServers == null) {
            controllerServers = Collections.emptyList();
        }
        String clientDnsLookupConfig = config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG);
        if (bootstrapServers.isEmpty()) {
            if (controllerServers.isEmpty()) {
                throw new ConfigException("You must set either " +
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + " or " +
                        AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG);
            } else {
                return new AdminBootstrapAddresses(true,
                    ClientUtils.parseAndValidateAddresses(controllerServers, clientDnsLookupConfig));
            }
        } else {
            if (controllerServers.isEmpty()) {
                return new AdminBootstrapAddresses(false,
                    ClientUtils.parseAndValidateAddresses(bootstrapServers, clientDnsLookupConfig));
            } else {
                throw new ConfigException("You cannot set both " +
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + " and " +
                        AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(usingBootstrapControllers, addresses);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!o.getClass().equals(AdminBootstrapAddresses.class))) return false;
        AdminBootstrapAddresses other = (AdminBootstrapAddresses) o;
        return usingBootstrapControllers == other.usingBootstrapControllers &&
            addresses.equals(other.addresses);
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("AdminBootstrapAddresses");
        bld.append("(usingBoostrapControllers=").append(usingBootstrapControllers);
        bld.append(", addresses=[");
        String prefix = "";
        for (InetSocketAddress address : addresses) {
            bld.append(prefix).append(address);
            prefix = ", ";
        }
        bld.append("])");
        return bld.toString();
    }
}
