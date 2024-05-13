This document describes a custom JUnit extension which allows for running the same JUnit tests against multiple Kafka 
cluster configurations.

# Annotations

A new `@ClusterTest` annotation is introduced which allows for a test to declaratively configure an underlying Kafka cluster.

```scala
@ClusterTest
def testSomething(): Unit = { ... }
```

This annotation has fields for a set of cluster types and number of brokers, as well as commonly parameterized configurations. 
Arbitrary server properties can also be provided in the annotation:

```java
@ClusterTest(types = {Type.Zk}, securityProtocol = "PLAINTEXT", properties = {
  @ClusterProperty(key = "inter.broker.protocol.version", value = "2.7-IV2"),
  @ClusterProperty(key = "socket.send.buffer.bytes", value = "10240"),
})
void testSomething() { ... }
```

Multiple `@ClusterTest` annotations can be given to generate more than one test invocation for the annotated method.

```scala
@ClusterTests(Array(
    @ClusterTest(securityProtocol = "PLAINTEXT"),
    @ClusterTest(securityProtocol = "SASL_PLAINTEXT")
))
def testSomething(): Unit = { ... }
```

A class-level `@ClusterTestDefaults` annotation is added to provide default values for `@ClusterTest` defined within 
the class. The intention here is to reduce repetitive annotation declarations and also make changing defaults easier 
for a class with many test cases.

# Dynamic Configuration

In order to allow for more flexible cluster configuration, a `@ClusterTemplate` annotation is also introduced. This 
annotation takes a single string value which references a static method on the test class. This method is used to 
produce any number of test configurations using a fluent builder style API.

```java
@ClusterTemplate("generateConfigs")
void testSomething() { ... }

static void generateConfigs(ClusterGenerator clusterGenerator) {
  clusterGenerator.accept(ClusterConfig.defaultClusterBuilder()
      .name("Generated Test 1")
      .serverProperties(props1)
      .ibp("2.7-IV1")
      .build());
  clusterGenerator.accept(ClusterConfig.defaultClusterBuilder()
      .name("Generated Test 2")
      .serverProperties(props2)
      .ibp("2.7-IV2")
      .build());
  clusterGenerator.accept(ClusterConfig.defaultClusterBuilder()
      .name("Generated Test 3")
      .serverProperties(props3)
      .build());
}
```

This "escape hatch" from the simple declarative style configuration makes it easy to dynamically configure clusters.


# JUnit Extension

One thing to note is that our "test*" methods are no longer _tests_, but rather they are test templates. We have added 
a JUnit extension called `ClusterTestExtensions` which knows how to process these annotations in order to generate test 
invocations. Test classes that wish to make use of these annotations need to explicitly register this extension:

```scala
import kafka.test.junit.ClusterTestExtensions

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ApiVersionsRequestTest {
   ...
}
```

# JUnit Lifecycle

The lifecycle of a test class that is extended with `ClusterTestExtensions` follows:

* JUnit discovers test template methods that are annotated with `@ClusterTest`, `@ClusterTests`, or `@ClusterTemplate`
* `ClusterTestExtensions` is called for each of these template methods in order to generate some number of test invocations

For each generated invocation:
* Static `@BeforeAll` methods are called
* Test class is instantiated
* Non-static `@BeforeEach` methods are called
* Kafka Cluster is started
* Test method is invoked
* Kafka Cluster is stopped
* Non-static `@AfterEach` methods are called
* Static `@AfterAll` methods are called

`@BeforeEach` methods give an opportunity to setup additional test dependencies before the cluster is started. 

# Dependency Injection

The class is introduced to provide context to the underlying cluster and to provide reusable functionality that was
previously garnered from the test hierarchy.

* ClusterInstance: a shim to the underlying class that actually runs the cluster, provides access to things like SocketServers

In order to inject the object, simply add it as a parameter to your test class, `@BeforeEach` method, or test method.

| Injection | Class | BeforeEach | Test | Notes
| --- | --- | --- | --- | --- |
| ClusterInstance | yes* | no | yes | Injectable at class level for convenience, can only be accessed inside test |

# Gotchas
* Test methods annotated with JUnit's `@Test` will still be run, but no cluster will be started and no dependency 
  injection will happen. This is generally not what you want.
* Even though ClusterConfig is accessible, it is immutable inside the test method.
