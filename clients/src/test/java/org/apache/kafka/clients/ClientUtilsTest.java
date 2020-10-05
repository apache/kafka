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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class ClientUtilsTest {


    @Test
    public void testParseAndValidateAddresses() throws UnknownHostException {
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
        List<InetSocketAddress> validatedAddresses = checkWithLookup(Arrays.asList("example.com:10000"));
        assertTrue("Unexpected addresses " + validatedAddresses, validatedAddresses.size() >= 1);
        List<String> validatedHostNames = validatedAddresses.stream().map(InetSocketAddress::getHostName)
                .collect(Collectors.toList());
        List<String> expectedHostNames = Arrays.asList("93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946");
        assertTrue("Unexpected addresses " + validatedHostNames, expectedHostNames.containsAll(validatedHostNames));
        validatedAddresses.forEach(address -> assertEquals(10000, address.getPort()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConfig() {
        ClientUtils.parseAndValidateAddresses(Arrays.asList("localhost:10000"), "random.value");
    }

    @Test(expected = ConfigException.class)
    public void testNoPort() {
        checkWithoutLookup("127.0.0.1");
    }

    @Test(expected = ConfigException.class)
    public void testOnlyBadHostname() {
        checkWithoutLookup("some.invalid.hostname.foo.bar.local:9999");
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

    @Test(expected = UnknownHostException.class)
    public void testResolveUnknownHostException() throws UnknownHostException {
        ClientUtils.resolve("some.invalid.hostname.foo.bar.local", ClientDnsLookup.USE_ALL_DNS_IPS);
    }

    @Test
    public void testResolveDnsLookup() throws UnknownHostException {
        // Note that kafka.apache.org resolves to at least 2 IP addresses
        assertEquals(1, ClientUtils.resolve("kafka.apache.org", ClientDnsLookup.DEFAULT).size());
    }

    @Test
    public void testResolveDnsLookupAllIps() throws UnknownHostException {
        // Note that kafka.apache.org resolves to at least 2 IP addresses
        assertTrue(ClientUtils.resolve("kafka.apache.org", ClientDnsLookup.USE_ALL_DNS_IPS).size() > 1);
    }

    @Test
    public void testResolveDnsLookupResolveCanonicalBootstrapServers() throws UnknownHostException {
        // Note that kafka.apache.org resolves to at least 2 IP addresses
        assertTrue(ClientUtils.resolve("kafka.apache.org", ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY).size() > 1);
    }

    private List<InetSocketAddress> checkWithoutLookup(String... url) {
        return ClientUtils.parseAndValidateAddresses(Arrays.asList(url), ClientDnsLookup.USE_ALL_DNS_IPS);
    }

    private List<InetSocketAddress> checkWithLookup(List<String> url) {
        return ClientUtils.parseAndValidateAddresses(url, ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY);
    }

}
