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

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ClientUtilsTest {

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

        String hostname = "example.com";
        Integer port = 10000;
        String canonicalHostname1 = "canonical_hostname1";
        String canonicalHostname2 = "canonical_hostname2";
        try (final MockedStatic<InetAddress> inetAddress = mockStatic(InetAddress.class)) {
            InetAddress inetAddress1 = mock(InetAddress.class);
            when(inetAddress1.getCanonicalHostName()).thenReturn(canonicalHostname1);
            InetAddress inetAddress2 = mock(InetAddress.class);
            when(inetAddress2.getCanonicalHostName()).thenReturn(canonicalHostname2);
            inetAddress.when(() -> InetAddress.getAllByName(hostname))
                .thenReturn(new InetAddress[]{inetAddress1, inetAddress2});
            try (MockedConstruction<InetSocketAddress> inetSocketAddress =
                     mockConstruction(
                         InetSocketAddress.class,
                         (mock, context) -> {
                             when(mock.isUnresolved()).thenReturn(false);
                             when(mock.getHostName()).thenReturn((String) context.arguments().get(0));
                             when(mock.getPort()).thenReturn((Integer) context.arguments().get(1));
                         })
            ) {
                List<InetSocketAddress> validatedAddresses = checkWithLookup(Collections.singletonList(hostname + ":" + port));
                assertEquals(2, inetSocketAddress.constructed().size());
                assertEquals(2, validatedAddresses.size());
                assertTrue(validatedAddresses.containsAll(List.of(
                    inetSocketAddress.constructed().get(0),
                    inetSocketAddress.constructed().get(1)))
                );
                validatedAddresses.forEach(address -> assertEquals(port, address.getPort()));
                validatedAddresses.stream().map(InetSocketAddress::getHostName).forEach(
                    hostName -> assertTrue(List.of(canonicalHostname1, canonicalHostname2).contains(hostName))
                );
            }
        }
    }

    @Test
    public void testValidBrokerAddress() {
        List<String> validBrokerAddress = List.of("localhost:9997", "localhost:9998", "localhost:9999");
        assertDoesNotThrow(() -> ClientUtils.parseAndValidateAddresses(validBrokerAddress, ClientDnsLookup.USE_ALL_DNS_IPS));
    }

    static Stream<List<String>> provideInvalidBrokerAddressTestCases() {
        return Stream.of(
            List.of("localhost:9997\nlocalhost:9998\nlocalhost:9999"),
            List.of("localhost:9997", "localhost:9998", " localhost:9999"),
            // Intentionally provide a single string, as users may provide space-separated brokers, which will be parsed as a single string.
            List.of("localhost:9997 localhost:9998 localhost:9999")
        );
    }

    @ParameterizedTest
    @MethodSource("provideInvalidBrokerAddressTestCases")
    public void testInvalidBrokerAddress(List<String> addresses) {
        assertThrows(ConfigException.class,
            () -> ClientUtils.parseAndValidateAddresses(addresses, ClientDnsLookup.USE_ALL_DNS_IPS));
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
    public void testInvalidPort() {
        assertThrows(ConfigException.class, () -> checkWithoutLookup("localhost:70000"));
    }

    @Test
    public void testOnlyBadHostname() {
        try (MockedConstruction<InetSocketAddress> inetSocketAddress =
                 mockConstruction(
                     InetSocketAddress.class,
                     (mock, context) -> when(mock.isUnresolved()).thenReturn(true)
                 )
        ) {
            Exception exception = assertThrows(
                ConfigException.class,
                () -> checkWithoutLookup("some.invalid.hostname.foo.bar.local:9999")
            );
            assertEquals(
                "No resolvable bootstrap urls given in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                exception.getMessage()
            );
        }
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
        HostResolver throwingHostResolver = host -> {
            throw new UnknownHostException();
        };
        assertThrows(
            UnknownHostException.class,
            () -> ClientUtils.resolve("some.invalid.hostname.foo.bar.local", throwingHostResolver)
        );
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
