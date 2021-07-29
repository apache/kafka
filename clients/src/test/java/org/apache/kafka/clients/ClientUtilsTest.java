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
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientUtilsTest {

    private final HostResolver hostResolver = new DefaultHostResolver();

    @Test
    public void testParseAndValidateAddresses() {
        checkWithoutLookup("127.0.0.1:8000");
        checkWithoutLookup("localhost:8080");
        checkWithoutLookup("[::1]:8000");
        checkWithoutLookup("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "localhost:10000");
        List<InetSocketAddress> validatedAddresses = checkWithoutLookup("localhost:10000");
        assertEquals(1, validatedAddresses.size());
        InetSocketAddress onlyAddress = validatedAddresses.get(0);
        assertEquals("localhost", onlyAddress.getHostName());
        assertEquals(10000, onlyAddress.getPort());
    }

    @Test
    public void testParseAndValidateAddressesWithReverseLookup() {
        checkWithoutLookup("127.0.0.1:8000");
        checkWithoutLookup("localhost:8080");
        checkWithoutLookup("[::1]:8000");
        checkWithoutLookup("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "localhost:10000");

        // With lookup of example.com, either one or two addresses are expected depending on
        // whether ipv4 and ipv6 are enabled
        List<InetSocketAddress> validatedAddresses = checkWithLookup(asList("example.com:10000"));
        assertTrue(validatedAddresses.size() >= 1, "Unexpected addresses " + validatedAddresses);
        List<String> validatedHostNames = validatedAddresses.stream().map(InetSocketAddress::getHostName)
                .collect(Collectors.toList());
        List<String> expectedHostNames = asList("93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946");
        assertTrue(expectedHostNames.containsAll(validatedHostNames), "Unexpected addresses " + validatedHostNames);
        validatedAddresses.forEach(address -> assertEquals(10000, address.getPort()));
    }

    @Test
    public void testInvalidConfig() {
        assertThrows(IllegalArgumentException.class,
            () -> ClientUtils.parseAndValidateAddresses(Collections.singletonList("localhost:10000"), "random.value"));
    }

    @Test
    public void testNoPort() {
        assertThrows(ConfigException.class, () -> checkWithoutLookup("127.0.0.1"));
    }

    @Test
    public void testOnlyBadHostname() {
        assertThrows(ConfigException.class, () -> checkWithoutLookup("some.invalid.hostname.foo.bar.local:9999"));
    }

    @Test
    public void testFilterPreferredAddresses() throws UnknownHostException {
        InetAddress ipv4 = InetAddress.getByName("192.0.0.1");
        InetAddress ipv6 = InetAddress.getByName("::1");

        InetAddress[] ipv4First = new InetAddress[]{ipv4, ipv6, ipv4};
        List<InetAddress> result = ClientUtils.filterPreferredAddresses(ipv4First);
        assertTrue(result.contains(ipv4));
        assertFalse(result.contains(ipv6));
        assertEquals(2, result.size());

        InetAddress[] ipv6First = new InetAddress[]{ipv6, ipv4, ipv4};
        result = ClientUtils.filterPreferredAddresses(ipv6First);
        assertTrue(result.contains(ipv6));
        assertFalse(result.contains(ipv4));
        assertEquals(1, result.size());
    }

    @Test
    public void testResolveUnknownHostException() {
        assertThrows(UnknownHostException.class,
            () -> ClientUtils.resolve("some.invalid.hostname.foo.bar.local", hostResolver));
    }

    @Test
    public void testResolveDnsLookup() throws UnknownHostException {
        InetAddress[] addresses = new InetAddress[] {
            InetAddress.getByName("198.51.100.0"), InetAddress.getByName("198.51.100.5")
        };
        HostResolver hostResolver = new AddressChangeHostResolver(addresses, addresses);
        assertEquals(asList(addresses), ClientUtils.resolve("kafka.apache.org", hostResolver));
    }

    private List<InetSocketAddress> checkWithoutLookup(String... url) {
        return ClientUtils.parseAndValidateAddresses(asList(url), ClientDnsLookup.USE_ALL_DNS_IPS);
    }

    private List<InetSocketAddress> checkWithLookup(List<String> url) {
        return ClientUtils.parseAndValidateAddresses(url, ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY);
    }

}
