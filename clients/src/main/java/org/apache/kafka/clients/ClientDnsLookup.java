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
package org.apache.kafka.clients;

import java.util.Locale;

public enum ClientDnsLookup {

    DEFAULT("default"),
    USE_ALL_DNS_IPS("use_all_dns_ips"),
    RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY("resolve_canonical_bootstrap_servers_only");

    private final String clientDnsLookup;

    ClientDnsLookup(String clientDnsLookup) {
        this.clientDnsLookup = clientDnsLookup;
    }

    @Override
    public String toString() {
        return clientDnsLookup;
    }

    public static ClientDnsLookup forConfig(String config) {
        return ClientDnsLookup.valueOf(config.toUpperCase(Locale.ROOT));
    }
}
