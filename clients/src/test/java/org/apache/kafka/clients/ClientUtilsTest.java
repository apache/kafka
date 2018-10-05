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

import org.apache.kafka.common.config.ClientDnsLookup;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

public class ClientUtilsTest {

    @Test
    public void testParseAndValidateAddresses() {
        check("127.0.0.1:8000");
        check("mydomain.com:8080");
        check("[::1]:8000");
        check("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "mydomain.com:10000");
        List<InetSocketAddress> validatedAddresses = check("some.invalid.hostname.foo.bar.local:9999", "mydomain.com:10000");
        assertEquals(1, validatedAddresses.size());
        InetSocketAddress onlyAddress = validatedAddresses.get(0);
        assertEquals("mydomain.com", onlyAddress.getHostName());
        assertEquals(10000, onlyAddress.getPort());
    }

    @Test(expected = ConfigException.class)
    public void testNoPort() {
        check("127.0.0.1");
    }

    @Test(expected = ConfigException.class)
    public void testOnlyBadHostname() {
        check("some.invalid.hostname.foo.bar.local:9999");
    }

    @Test
    public void testFilterPreferedAddresses() throws UnknownHostException {
        InetAddress ipv4 = InetAddress.getByName("192.0.0.1");
        InetAddress ipv6 = InetAddress.getByName("::1");

        InetAddress[] ipv4First = new InetAddress[]{ipv4, ipv6, ipv4};
        List<InetAddress> result = ClientUtils.filterPreferedAddresses(ipv4First);
        assertTrue(result.contains(ipv4));
        assertFalse(result.contains(ipv6));
        assertEquals(2, result.size());

        InetAddress[] ipv6First = new InetAddress[]{ipv6, ipv4, ipv4};
        result = ClientUtils.filterPreferedAddresses(ipv6First);
        assertTrue(result.contains(ipv6));
        assertFalse(result.contains(ipv4));
        assertEquals(1, result.size());
    }

    @Test(expected = UnknownHostException.class)
    public void testResolveUnknownHostException() throws UnknownHostException {
        ClientUtils.resolve("some.invalid.hostname.foo.bar.local", ClientDnsLookup.DEFAULT);
    }

    @Test
    public void testResolveDnsLookup() throws UnknownHostException {
        assertEquals(1, ClientUtils.resolve("localhost", ClientDnsLookup.DEFAULT).size());
    }

    @Test
    public void testResolveDnsLookupAllIps() throws UnknownHostException {
        assertEquals(2, ClientUtils.resolve("kafka.apache.org", ClientDnsLookup.USE_ALL_DNS_IPS).size());
    }

    private List<InetSocketAddress> check(String... url) {
        return ClientUtils.parseAndValidateAddresses(Arrays.asList(url));
    }
}
