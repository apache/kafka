This document describes a custom JUnit extension which allows for running the same JUnit tests against multiple Kafka 
cluster configurations.

# Annotations

Three annotations are provided for defining a template of a Kafka cluster.

* `@ClusterTest`: declarative style cluster definition
* `@ClusterTests`: wrapper around multiple `@ClusterTest`-s
* `@ClusterTemplate`: points to a function for imperative cluster definition

Another helper annotation `@ClusterTestDefaults` allows overriding the defaults for 
all `@ClusterTest` in a single test class.

# Usage

The simplest usage is `@ClusterTest` by itself which will use some reasonable defaults.

```java
public class SampleTest {
    @ClusterTest
    void testSomething() { ... }
}
```

The defaults can be modified by setting specific parameters on the annotation. 

```java
public class SampleTest {
    @ClusterTest(brokers = 3, metadataVersion = MetadataVersion.IBP_4_0_IV3)
    void testSomething() { ... }
}
```

It is also possible to modify the defaults for a whole class using `@ClusterTestDefaults`.

```java
@ClusterTestDefaults(brokers = 3, metadataVersion = MetadataVersion.IBP_4_0_IV3)
public class SampleTest {
    @ClusterTest
    void testSomething() { ... }
}
```

To set some specific config, an array of `@ClusterProperty` annotations can be
given.

```java
public class SampleTest {
    @ClusterTest(
      types = {Type.KRAFT},
      brokerSecurityProtocol = SecurityProtocol.PLAINTEXT,
      properties = {
          @ClusterProperty(key = "inter.broker.protocol.version", value = "2.7-IV2"),
          @ClusterProperty(key = "socket.send.buffer.bytes", value = "10240"),
    })
    void testSomething() { ... }
}
```

Using the `@ClusterTests` annotation, multiple declarative cluster templates can
be given.

```java
public class SampleTest {
    @ClusterTests({
        @ClusterTest(brokerSecurityProtocol = SecurityProtocol.PLAINTEXT),
        @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT)
    })
    void testSomething() { ... }
}
```

# Dynamic Configuration

In order to allow for more flexible cluster configuration, a `@ClusterTemplate` 
annotation is also introduced. This annotation takes a single string value which 
references a static method on the test class. This method is used to produce any 
number of test configurations using a fluent builder style API.

```java
import java.util.List;

@ClusterTemplate("generateConfigs")
void testSomething() { ... }

static List<ClusterConfig> generateConfigs() {
  ClusterConfig config1 = ClusterConfig.defaultClusterBuilder()
          .name("Generated Test 1")
          .serverProperties(props1)
          .setMetadataVersion(MetadataVersion.IBP_2_7_IV1)
          .build();
  ClusterConfig config2 = ClusterConfig.defaultClusterBuilder()
          .name("Generated Test 2")
          .serverProperties(props2)
          .setMetadataVersion(MetadataVersion.IBP_2_7_IV2)
          .build();
  ClusterConfig config3 = ClusterConfig.defaultClusterBuilder()
          .name("Generated Test 3")
          .serverProperties(props3)
          .build();
  return List.of(config1, config2, config3);
}
```

This alternate configuration style makes it easy to create any number of complex 
configurations. Each returned ClusterConfig by a template method will result in
an additional variation of the run.


# JUnit Extension

The core logic of our test framework lies in `ClusterTestExtensions` which is a
JUnit extension. It is automatically registered using SPI and will look for test
methods that include one of the three annotations mentioned above. 

This way of dynamically generating tests uses the JUnit concept of test templates.

# JUnit Lifecycle

JUnit discovers test template methods that are annotated with `@ClusterTest`, 
`@ClusterTests`, or `@ClusterTemplate`. These annotations are processed and some
number of test invocations are created.

For each generated test invocation we have the following lifecycle:

* Static `@BeforeAll` methods are called
* Test class is instantiated
* Kafka Cluster is started (if autoStart=true)
* Non-static `@BeforeEach` methods are called
* Test method is invoked
* Kafka Cluster is stopped
* Non-static `@AfterEach` methods are called
* Static `@AfterAll` methods are called

`@BeforeEach` methods give an opportunity to set up additional test dependencies 
after the cluster has started but before the test method is run.

# Dependency Injection

A ClusterInstance object can be injected into the test method or the test class constructor.
This object is a shim to the underlying test framework and provides access to things like 
SocketServers and has convenience factory methods for getting a client.

The class is introduced to provide context to the underlying cluster and to provide reusable 
functionality that was previously garnered from the test hierarchy.

Common usage is to inject this class into a test method

```java
class SampleTest {
    @ClusterTest
    public void testOne(ClusterInstance cluster) {
        this.cluster.admin().createTopics(...);
        // Test code
    }
}
```

For cases where there is common setup code that involves the cluster (such as 
creating topics), it is possible to access the ClusterInstance from a `@BeforeEach` 
method. This requires injecting the object in the constructor. For example, 

```java
class SampleTest {
    private final ClusterInstance cluster;

    SampleTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }
    
    @BeforeEach
    public void setup() {
      // Common setup code with started ClusterInstance
      this.cluster.admin().createTopics(...); 
    }

    @ClusterTest
    public void testOne() {
        // Test code
    }
}
```

It is okay to inject the ClusterInstance in both ways. The same object will be
provided in either case.

# Gotchas
* Cluster tests are not compatible with other test templates like `@ParameterizedTest`
* Test methods annotated with JUnit's `@Test` will still be run, but no cluster will be started and no dependency 
  injection will happen. This is generally not what you want.
* Even though ClusterConfig is accessible, it is immutable inside the test method.
