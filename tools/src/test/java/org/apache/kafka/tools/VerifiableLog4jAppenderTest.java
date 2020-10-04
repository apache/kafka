package org.apache.kafka.tools;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.tools.VerifiableLog4jAppender.VerifiableLog4jAppenderOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class VerifiableLog4jAppenderTest {

  private final String topic = "topic";
  private final String brokerList = "localhost:9091";
  private final String maxMessages = "100";
  private final String acks = "1";
  private final String securityProtocol = "SASL_SSL";
  private final String sslTruststoreLocation = "/";
  private final String sslTruststorePassword = "passw";
  private final String appenderConfig = "/config.props";
  private final String saslKerberosServiceName = "service";
  private final String clientJaasConfPath = "/client.jaas";
  private final String kerb5ConfPath = "/kerb5.conf";

  private final String[] requiredArgs = new String[] {
      "--topic", topic,
      "--bootstrap-server", brokerList
  };

  private final String[] nonRequiredArgs = new String[] {
      "--max-messages", maxMessages,
      "--acks", acks,
      "--security-protocol", securityProtocol,
      "--ssl-truststore-location", sslTruststoreLocation,
      "--ssl-truststore-password", sslTruststorePassword,
      "--appender.config", appenderConfig,
      "--sasl-kerberos-service-name", saslKerberosServiceName,
      "--client-jaas-conf-path", clientJaasConfPath,
      "--kerb5-conf-path", kerb5ConfPath
  };

  private final String[] args = Stream.concat(Arrays.stream(requiredArgs), Arrays.stream(nonRequiredArgs)).toArray(String[]::new);

  @Before
  public void setUp() {
    Exit.setExitProcedure((statusCode, message) -> {
      throw new IllegalArgumentException(message);
    });
  }

  @After
  public void tearDown() {
    Exit.resetExitProcedure();
  }

  @Test
  public void testOptionsWorkCorrectly() {
    VerifiableLog4jAppenderOptions opts = VerifiableLog4jAppenderOptions.parse(args);
    assertEquals(topic, opts.topic);
    assertEquals(brokerList, opts.brokerList);
    assertEquals(Integer.parseInt(maxMessages), (int) opts.maxMessages);
    assertEquals(acks, opts.acks);
    assertEquals(securityProtocol, opts.securityProtocol);
    assertEquals(sslTruststoreLocation, opts.sslTruststoreLocation);
    assertEquals(sslTruststorePassword, opts.sslTruststorePassword);
    assertEquals(appenderConfig, opts.configFile);
    assertEquals(saslKerberosServiceName, opts.saslKerberosServiceName);
    assertEquals(clientJaasConfPath, opts.clientJaasConfPath);
    assertEquals(kerb5ConfPath, opts.kerb5ConfPath);
  }

  @Test
  public void testOptionsDefaults() {
    VerifiableLog4jAppenderOptions opts = VerifiableLog4jAppenderOptions.parse(requiredArgs);
    assertEquals(topic, opts.topic);
    assertEquals(brokerList, opts.brokerList);
    assertEquals(-1, (int) opts.maxMessages);
    assertEquals("-1", opts.acks);
    assertEquals("PLAINTEXT", opts.securityProtocol);
    assertNull(opts.sslTruststoreLocation);
    assertNull(opts.sslTruststorePassword);
    assertNull(opts.configFile);
    assertNull(opts.saslKerberosServiceName);
    assertNull(opts.clientJaasConfPath);
    assertNull(opts.kerb5ConfPath);
  }

  @Test
  public void testBrokerListArg() {
    VerifiableLog4jAppenderOptions opts = VerifiableLog4jAppenderOptions.parse(new String[] {
        "--topic", topic,
        "--broker-list", brokerList
    });
    assertEquals(brokerList, opts.brokerList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBrokerListArgAndBootstrapServerAreMutuallyExclusive() {
    VerifiableLog4jAppenderOptions.parse(new String[] {
        "--topic", topic,
        "--bootstrap-server", brokerList,
        "--broker-list", brokerList
    });
  }
}
