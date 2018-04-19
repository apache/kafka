.. _streams_developer-guide_security:

`Streams Security  <#streams-security>`__
=========================================

.. contents:: Table of Contents
   :local:

Kafka Streams natively integrates with the `Kafka’s security
features <../../kafka/security.html#kafka-security>`__ and supports all
of the client-side security features in Kafka. Streams leverages the
`Java Producer and Consumer
API <../../clients/index.html#kafka-clients>`__.

To secure your Stream processing applications, configure the security
settings in the corresponding Kafka producer and consumer clients, and
then specify the corresponding configuration settings in your Kafka
Streams application.

Kafka supports cluster encryption and authentication, including a mix of
authenticated and unauthenticated, and encrypted and non-encrypted
clients. Using security is optional.

Here a few relevant client-side security features:

Encrypt data-in-transit between your applications and Kafka brokers
    You can enable the encryption of the client-server communication
    between your applications and the Kafka brokers. For example, you
    can configure your applications to always use encryption when
    reading and writing data to and from Kafka. This is critical when
    reading and writing data across security domains such as internal
    network, public internet, and partner networks.
Client authentication
    You can enable client authentication for connections from your
    application to Kafka brokers. For example, you can define that only
    specific applications are allowed to connect to your Kafka cluster.
Client authorization
    You can enable client authorization of read and write operations by
    your applications. For example, you can define that only specific
    applications are allowed to read from a Kafka topic. You can also
    restrict write access to Kafka topics to prevent data pollution or
    fraudulent activities.

For more information about the security features in Apache Kafka, see
`Kafka Security <../../kafka/security.html#kafka-security>`__.

`Required ACL setting for secure Kafka clusters <#required-acl-setting-for-secure-kafka-clusters>`__
----------------------------------------------------------------------------------------------------

When applications are run against a secured Kafka cluster, the principal
running the application must have the ACL
``--cluster --operation Create`` set so that the application has the
permissions to create `internal
topics <manage-topics.html#streams-developer-guide-topics-internal>`__.

`Security example <#security-example>`__
----------------------------------------

The purpose is to configure a Kafka Streams application to enable client
authentication and encrypt data-in-transit when communicating with its
Kafka cluster.

This example assumes that the Kafka brokers in the cluster already have
their security setup and that the necessary SSL certificates are
available to the application in the local filesystem locations. For
example, if you are using Docker then you must also include these SSL
certificates in the correct locations within the Docker image.

The snippet below shows the settings to enable client authentication and
SSL encryption for data-in-transit between your Kafka Streams
application and the Kafka cluster it is reading and writing from:

.. code:: bash

    # Essential security settings to enable client authentication and SSL encryption
    bootstrap.servers=kafka.example.com:9093
    security.protocol=SSL
    ssl.truststore.location=/etc/security/tls/kafka.client.truststore.jks
    ssl.truststore.password=test1234
    ssl.keystore.location=/etc/security/tls/kafka.client.keystore.jks
    ssl.keystore.password=test1234
    ssl.key.password=test1234

Configure these settings in the application for your ``StreamsConfig``
instance. These settings will encrypt any data-in-transit that is being
read from or written to Kafka, and your application will authenticate
itself against the Kafka brokers that it is communicating with. Note
that this example does not cover client authorization.

.. code:: java

    // Code of your Java application that uses the Kafka Streams library
    Properties settings = new Properties();
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "secure-kafka-streams-app");
    // Where to find secure Kafka brokers.  Here, it's on port 9093.
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.example.com:9093");
    //
    // ...further non-security related settings may follow here...
    //
    // Security settings.
    // 1. These settings must match the security settings of the secure Kafka cluster.
    // 2. The SSL trust store and key store files must be locally accessible to the application.
    settings.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    settings.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/security/tls/kafka.client.truststore.jks");
    settings.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "test1234");
    settings.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/security/tls/kafka.client.keystore.jks");
    settings.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "test1234");
    settings.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "test1234");
    StreamsConfig streamsConfiguration = new StreamsConfig(settings);

If you incorrectly configure a security setting in your application, it
will fail at runtime, typically right after you start it. For example,
if you enter an incorrect password for the ``ssl.keystore.password``
setting, an error message similar to this would be logged and then the
application would terminate:

.. code:: bash

    # Misconfigured ssl.keystore.password
    Exception in thread "main" org.apache.kafka.common.KafkaException: Failed to construct kafka producer
    [...snip...]
    Caused by: org.apache.kafka.common.KafkaException: org.apache.kafka.common.KafkaException:
       java.io.IOException: Keystore was tampered with, or password was incorrect
    [...snip...]
    Caused by: java.security.UnrecoverableKeyException: Password verification failed


Monitor your Kafka Streams application log files for such error messages
to spot any misconfigured applications quickly.

