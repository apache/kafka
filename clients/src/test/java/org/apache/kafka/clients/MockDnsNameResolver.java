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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class MockDnsNameResolver implements DnsNameResolver {
    private final Map<String, InetAddress[]> knownMappings;
    private final Set<String> unknownHostnames;

    private MockDnsNameResolver(Map<String, InetAddress[]> knownMappings, Set<String> unknownHostnames) {
        this.knownMappings = knownMappings;
        this.unknownHostnames = unknownHostnames;
    }

    @Override
    public InetAddress[] resolve(String host) throws UnknownHostException {
        if (unknownHostnames.contains(host)) {
            throw new UnknownHostException(host);
        }
        InetAddress[] addresses = knownMappings.get(host);
        if (addresses == null) {
            throw new IllegalStateException("Unexpected DnsNameResolver::resolve(" + host + ") call");
        }
        return addresses;
    }

    static class Builder {
        private final Map<String, InetAddress[]> knownMappings = new HashMap<>();
        private final Set<String> unknownHostnames = new HashSet<>();

        Builder withMapping(String hostname, InetAddress... addresses) {
            knownMappings.put(hostname, addresses);
            return this;
        }

        Builder withUnknownHostname(String hostname) {
            unknownHostnames.add(hostname);
            return this;
        }

        DnsNameResolver build() {
            return new MockDnsNameResolver(knownMappings, unknownHostnames);
        }
    }
}
