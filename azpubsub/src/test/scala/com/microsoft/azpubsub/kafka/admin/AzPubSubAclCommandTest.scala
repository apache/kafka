package com.microsoft.azpubsub.kafka.admin
import java.io.ByteArrayOutputStream

import org.apache.kafka.common.internals.FatalExitError
import org.junit.{After, Before, Test}
import org.junit.Assert._
import kafka.utils.Exit
import org.apache.kafka.common.acl.{AccessControlEntry, AclOperation, AclPermissionType}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.resource.{ResourceType, ResourcePatternFilter, PatternType, ResourcePattern};

class AzPubSubAclCommandTest {

  @Before
  def setUp(): Unit = Exit.setExitProcedure((status, _) => throw new FatalExitError(status))

  @After
  def tearDown(): Unit = Exit.resetExitProcedure()

  @Test
  def testOpts(): Unit = {
    var args = Array("--bootstrap-server", "localhost:9092", "--list", "--principal", "User:Alice", "--topic", "test", "--pc")
    val opts = new AzPubSubAclCommandOptions(args)

    assertTrue(opts.options.has(opts.bootstrapServerOpt))
    assertTrue(opts.options.has(opts.listOpt))
    assertTrue(opts.options.has(opts.listPrincipalsOpt))
    assertTrue(opts.options.has(opts.topicOpt))
    assertTrue(opts.options.has(opts.outputAsProducerConsumerOpt))
  }

  @Test
  def testPrintAcls(): Unit = {
    var args = Array("--bootstrap-server" , "localhost:9092", "--list", "--topic", "test", "--pc")
    val opts = new AzPubSubAclCommandOptions(args)
    val aclCommandService = new AzPubSubAdminClientService(opts)
    val filters = Set(new ResourcePatternFilter(ResourceType.TOPIC, "test", PatternType.LITERAL))
    val listPrincipals = Set(new KafkaPrincipal("User", "Alice"))
    val resourceToAcls = Map(new ResourcePattern(ResourceType.TOPIC, "test", PatternType.LITERAL) ->
                            Set(new AccessControlEntry("User:Alice", "*", AclOperation.READ,
                              AclPermissionType.ALLOW), new AccessControlEntry("User:Alice", "*", AclOperation.WRITE,
                              AclPermissionType.ALLOW), new AccessControlEntry("User:Alice", "*", AclOperation.CREATE,
                              AclPermissionType.ALLOW), new AccessControlEntry("User:Alice", "*", AclOperation.DESCRIBE,
                              AclPermissionType.ALLOW)), (new ResourcePattern(ResourceType.GROUP, "testgroup", PatternType.LITERAL) ->
                              Set(new AccessControlEntry("User:Alice", "*", AclOperation.READ, AclPermissionType.ALLOW))))
    val (output) = grabConsoleOutput(aclCommandService.printAcls(filters, listPrincipals, resourceToAcls))

    assertTrue(output.contains( """{"operation":"ANY","permissionType":"ALLOW","aggregatedOperation":"PRODUCER","host":"*","principal":"User:Alice"}"""))
    assertTrue(output.contains( """{"operation":"ANY","permissionType":"ALLOW","aggregatedOperation":"CONSUMER","host":"*","principal":"User:Alice"}"""))
    assertTrue(output.contains( """{"operation":"ANY","permissionType":"ALLOW","aggregatedOperation":"GROUP","host":"*","principal":"User:Alice"}"""))

    //Assert no stray acls
    assertFalse(output.contains( """"operation":"READ""""))
    assertFalse(output.contains( """"operation":"WRITE""""))
    assertFalse(output.contains( """"operation":"CREATE""""))
    assertFalse(output.contains( """"operation":"Describe""""))
  }

  @Test
  def testPrintStrayAcls(): Unit = {
    var args = Array("--bootstrap-server", "localhost:9092", "--list", "--topic", "test", "--topic", "test2", "--pc", "--principal", "User:Alice", "--principal", "User:Bob" )
    val opts = new AzPubSubAclCommandOptions(args)
    val aclCommandService = new AzPubSubAdminClientService(opts)
    val filters = Set(new ResourcePatternFilter(ResourceType.TOPIC, "test", PatternType.LITERAL),
      new ResourcePatternFilter(ResourceType.TOPIC, "test2", PatternType.LITERAL))
    val listPrincipals = Set(new KafkaPrincipal("User", "Alice"), new KafkaPrincipal("User", "Bob"))
    val resourceToAcls = Map(new ResourcePattern(ResourceType.TOPIC, "test", PatternType.LITERAL) ->
      Set(new AccessControlEntry("User:Alice", "*", AclOperation.READ,
        AclPermissionType.ALLOW), new AccessControlEntry("User:Alice", "*", AclOperation.WRITE,
        AclPermissionType.ALLOW), new AccessControlEntry("User:Alice", "*", AclOperation.DESCRIBE,
        AclPermissionType.ALLOW), new AccessControlEntry("User:Bob", "*", AclOperation.READ,
        AclPermissionType.ALLOW)), (new ResourcePattern(ResourceType.TOPIC, "test2", PatternType.LITERAL) ->
      Set(new AccessControlEntry("User:Alice", "*", AclOperation.READ, AclPermissionType.ALLOW))))
    val (output) = grabConsoleOutput(aclCommandService.printAcls(filters, listPrincipals, resourceToAcls))

    assertTrue(output.contains( """{"operation":"WRITE","permissionType":"ALLOW","aggregatedOperation":"NONE","host":"*","principal":"User:Alice"}"""))
    assertTrue(output.contains( """{"operation":"READ","permissionType":"ALLOW","aggregatedOperation":"NONE","host":"*","principal":"User:Bob"}"""))
    assertTrue(output.contains( """{"operation":"ANY","permissionType":"ALLOW","aggregatedOperation":"CONSUMER","host":"*","principal":"User:Alice"}"""))
    assertTrue(output.contains( """{"operation":"READ","permissionType":"ALLOW","aggregatedOperation":"NONE","host":"*","principal":"User:Alice"}"""))

    assertTrue(output.contains( """"patternType":"LITERAL","resourceType":"TOPIC","name":"test""""))
    assertTrue(output.contains( """"patternType":"LITERAL","resourceType":"TOPIC","name":"test2""""))

  }

    def grabConsoleOutput(f: => Unit) : String = {
    val out = new ByteArrayOutputStream
    try scala.Console.withOut(out)(f)
    finally scala.Console.out.flush()
    out.toString
  }
}
