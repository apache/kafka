/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.util.Properties
import joptsimple._
import joptsimple.util.EnumConverter
import kafka.security.authorizer.{AclAuthorizer, AclEntry}
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, Resource => JResource, ResourceType => JResourceType}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Utils, SecurityUtils => JSecurityUtils}
import org.apache.kafka.security.authorizer.AuthorizerUtils
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.io.StdIn

object AclCommand extends Logging {

  private val AuthorizerDeprecationMessage: String = "Warning: support for ACL configuration directly " +
    "through the authorizer is deprecated and will be removed in a future release. Please use " +
    "--bootstrap-server instead to set ACLs through the admin client."
  private val ClusterResourceFilter = new ResourcePatternFilter(JResourceType.CLUSTER, JResource.CLUSTER_NAME, PatternType.LITERAL)

  private val Newline = scala.util.Properties.lineSeparator

  def main(args: Array[String]): Unit = {

    val opts = new AclCommandOptions(args)

    CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to manage acls on kafka.")

    opts.checkArgs()

    val aclCommandService = {
      if (opts.options.has(opts.bootstrapServerOpt)) {
        new AdminClientService(opts)
      } else {
        val authorizerClassName = if (opts.options.has(opts.authorizerOpt))
          opts.options.valueOf(opts.authorizerOpt)
        else
          classOf[AclAuthorizer].getName

        new AuthorizerService(authorizerClassName, opts)
      }
    }

    try {
      if (opts.options.has(opts.addOpt))
        aclCommandService.addAcls()
      else if (opts.options.has(opts.removeOpt))
        aclCommandService.removeAcls()
      else if (opts.options.has(opts.listOpt))
        aclCommandService.listAcls()
    } catch {
      case e: Throwable =>
        println(s"Error while executing ACL command: ${e.getMessage}")
        println(Utils.stackTrace(e))
        Exit.exit(1)
    }
  }

  sealed trait AclCommandService {
    def addAcls(): Unit
    def removeAcls(): Unit
    def listAcls(): Unit
  }

  private class AdminClientService(val opts: AclCommandOptions) extends AclCommandService with Logging {

    private def withAdminClient(opts: AclCommandOptions)(f: Admin => Unit): Unit = {
      val props = if (opts.options.has(opts.commandConfigOpt))
        Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
      else
        new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
      val adminClient = Admin.create(props)

      try {
        f(adminClient)
      } finally {
        adminClient.close()
      }
    }

    def addAcls(): Unit = {
      val resourceToAcl = getResourceToAcls(opts)
      withAdminClient(opts) { adminClient =>
        for ((resource, acls) <- resourceToAcl) {
          println(s"Adding ACLs for resource `$resource`: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
          val aclBindings = acls.map(acl => new AclBinding(resource, acl)).asJavaCollection
          adminClient.createAcls(aclBindings).all().get()
        }

        listAcls(adminClient)
      }
    }

    def removeAcls(): Unit = {
      withAdminClient(opts) { adminClient =>
        val filterToAcl = getResourceFilterToAcls(opts)

        for ((filter, acls) <- filterToAcl) {
          if (acls.isEmpty) {
            if (confirmAction(opts, s"Are you sure you want to delete all ACLs for resource filter `$filter`? (y/n)"))
              removeAcls(adminClient, acls, filter)
          } else {
            if (confirmAction(opts, s"Are you sure you want to remove ACLs: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline from resource filter `$filter`? (y/n)"))
              removeAcls(adminClient, acls, filter)
          }
        }

        listAcls(adminClient)
      }
    }

    def listAcls(): Unit = {
      withAdminClient(opts) { adminClient =>
        listAcls(adminClient)
      }
    }

    private def listAcls(adminClient: Admin): Unit = {
      val filters = getResourceFilter(opts, dieIfNoResourceFound = false)
      val listPrincipals = getPrincipals(opts, opts.listPrincipalsOpt)
      val resourceToAcls = getAcls(adminClient, filters)

      if (listPrincipals.isEmpty) {
        printResourceAcls(resourceToAcls)
      } else {
        listPrincipals.foreach{principal =>
          println(s"ACLs for principal `$principal`")
          val filteredResourceToAcls = resourceToAcls.map { case (resource, acls) =>
            resource -> acls.filter(acl => principal.toString.equals(acl.principal))
          }.filter { case (_, acls) => acls.nonEmpty }
          printResourceAcls(filteredResourceToAcls)
        }
      }
    }

    private def printResourceAcls(resourceToAcls: Map[ResourcePattern, Set[AccessControlEntry]]): Unit = {
      for ((resource, acls) <- resourceToAcls)
        println(s"Current ACLs for resource `$resource`: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
    }

    private def removeAcls(adminClient: Admin, acls: Set[AccessControlEntry], filter: ResourcePatternFilter): Unit = {
      if (acls.isEmpty)
        adminClient.deleteAcls(List(new AclBindingFilter(filter, AccessControlEntryFilter.ANY)).asJava).all().get()
      else {
        val aclBindingFilters = acls.map(acl => new AclBindingFilter(filter, acl.toFilter)).toList.asJava
        adminClient.deleteAcls(aclBindingFilters).all().get()
      }
    }

    private def getAcls(adminClient: Admin, filters: Set[ResourcePatternFilter]): Map[ResourcePattern, Set[AccessControlEntry]] = {
      val aclBindings =
        if (filters.isEmpty) adminClient.describeAcls(AclBindingFilter.ANY).values().get().asScala.toList
        else {
          val results = for (filter <- filters) yield {
            adminClient.describeAcls(new AclBindingFilter(filter, AccessControlEntryFilter.ANY)).values().get().asScala.toList
          }
          results.reduceLeft(_ ++ _)
        }

      val resourceToAcls = mutable.Map[ResourcePattern, Set[AccessControlEntry]]().withDefaultValue(Set())

      aclBindings.foreach(aclBinding => resourceToAcls(aclBinding.pattern()) = resourceToAcls(aclBinding.pattern()) + aclBinding.entry())
      resourceToAcls.toMap
    }
  }

  class AuthorizerService(val authorizerClassName: String, val opts: AclCommandOptions) extends AclCommandService with Logging {

    private def withAuthorizer()(f: Authorizer => Unit): Unit = {
      // It is possible that zookeeper.set.acl could be true without SASL if mutual certificate authentication is configured.
      // We will default the value of zookeeper.set.acl to true or false based on whether SASL is configured,
      // but if SASL is not configured and zookeeper.set.acl is supposed to be true due to mutual certificate authentication
      // then it will be up to the user to explicitly specify zookeeper.set.acl=true in the authorizer-properties.
      val defaultProps = Map(KafkaConfig.ZkEnableSecureAclsProp -> JaasUtils.isZkSaslEnabled)
      val authorizerPropertiesWithoutTls =
        if (opts.options.has(opts.authorizerPropertiesOpt)) {
          val authorizerProperties = opts.options.valuesOf(opts.authorizerPropertiesOpt)
          defaultProps ++ CommandLineUtils.parseKeyValueArgs(authorizerProperties, false).asScala
        } else {
          defaultProps
        }
      val authorizerProperties =
        if (opts.options.has(opts.zkTlsConfigFile)) {
          // load in TLS configs both with and without the "authorizer." prefix
          val validKeys = (KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.toList ++ KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.map("authorizer." + _).toList).asJava
          authorizerPropertiesWithoutTls ++ Utils.loadProps(opts.options.valueOf(opts.zkTlsConfigFile), validKeys).asInstanceOf[java.util.Map[String, Any]].asScala
        }
        else
          authorizerPropertiesWithoutTls

      val authZ = AuthorizerUtils.createAuthorizer(authorizerClassName)
      try {
        authZ.configure(authorizerProperties.asJava)
        f(authZ)
      }
      finally CoreUtils.swallow(authZ.close(), this)
    }

    def addAcls(): Unit = {
      val resourceToAcl = getResourceToAcls(opts)
      withAuthorizer() { authorizer =>
        for ((resource, acls) <- resourceToAcl) {
          println(s"Adding ACLs for resource `$resource`: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
          val aclBindings = acls.map(acl => new AclBinding(resource, acl))
          authorizer.createAcls(null,aclBindings.toList.asJava).asScala.map(_.toCompletableFuture.get).foreach { result =>
            result.exception.ifPresent { exception =>
              println(s"Error while adding ACLs: ${exception.getMessage}")
              println(Utils.stackTrace(exception))
            }
          }
        }

        listAcls()
      }
    }

    def removeAcls(): Unit = {
      withAuthorizer() { authorizer =>
        val filterToAcl = getResourceFilterToAcls(opts)

        for ((filter, acls) <- filterToAcl) {
          if (acls.isEmpty) {
            if (confirmAction(opts, s"Are you sure you want to delete all ACLs for resource filter `$filter`? (y/n)"))
              removeAcls(authorizer, acls, filter)
          } else {
            if (confirmAction(opts, s"Are you sure you want to remove ACLs: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline from resource filter `$filter`? (y/n)"))
              removeAcls(authorizer, acls, filter)
          }
        }

        listAcls()
      }
    }

    def listAcls(): Unit = {
      withAuthorizer() { authorizer =>
        val filters = getResourceFilter(opts, dieIfNoResourceFound = false)
        val listPrincipals = getPrincipals(opts, opts.listPrincipalsOpt)
        val resourceToAcls = getAcls(authorizer, filters)

        if (listPrincipals.isEmpty) {
          for ((resource, acls) <- resourceToAcls)
            println(s"Current ACLs for resource `$resource`: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
        } else {
          listPrincipals.foreach(principal => {
            println(s"ACLs for principal `$principal`")
            val filteredResourceToAcls =  resourceToAcls.map { case (resource, acls) =>
              resource -> acls.filter(acl => principal.toString.equals(acl.principal))
            }.filter { case (_, acls) => acls.nonEmpty }

            for ((resource, acls) <- filteredResourceToAcls)
              println(s"Current ACLs for resource `$resource`: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
          })
        }
      }
    }

    private def removeAcls(authorizer: Authorizer, acls: Set[AccessControlEntry], filter: ResourcePatternFilter): Unit = {
      val result = if (acls.isEmpty)
        authorizer.deleteAcls(null, List(new AclBindingFilter(filter, AccessControlEntryFilter.ANY)).asJava)
      else {
        val aclBindingFilters = acls.map(acl => new AclBindingFilter(filter, acl.toFilter)).toList.asJava
        authorizer.deleteAcls(null, aclBindingFilters)
      }
      result.asScala.map(_.toCompletableFuture.get).foreach { result =>
        result.exception.ifPresent { exception =>
          println(s"Error while removing ACLs: ${exception.getMessage}")
          println(Utils.stackTrace(exception))
        }
        result.aclBindingDeleteResults.forEach { deleteResult =>
          deleteResult.exception.ifPresent { exception =>
            println(s"Error while removing ACLs: ${exception.getMessage}")
            println(Utils.stackTrace(exception))
          }
        }
      }
    }

    private def getAcls(authorizer: Authorizer, filters: Set[ResourcePatternFilter]): Map[ResourcePattern, Set[AccessControlEntry]] = {
      val aclBindings =
        if (filters.isEmpty) authorizer.acls(AclBindingFilter.ANY).asScala
        else {
          val results = for (filter <- filters) yield {
            authorizer.acls(new AclBindingFilter(filter, AccessControlEntryFilter.ANY)).asScala
          }
          results.reduceLeft(_ ++ _)
        }

      val resourceToAcls = mutable.Map[ResourcePattern, Set[AccessControlEntry]]().withDefaultValue(Set())

      aclBindings.foreach(aclBinding => resourceToAcls(aclBinding.pattern()) = resourceToAcls(aclBinding.pattern()) + aclBinding.entry())
      resourceToAcls.toMap
    }
  }

  private def getResourceToAcls(opts: AclCommandOptions): Map[ResourcePattern, Set[AccessControlEntry]] = {
    val patternType = opts.options.valueOf(opts.resourcePatternType)
    if (!patternType.isSpecific)
      CommandLineUtils.printUsageAndExit(opts.parser, s"A '--resource-pattern-type' value of '$patternType' is not valid when adding acls.")

    val resourceToAcl = getResourceFilterToAcls(opts).map {
      case (filter, acls) =>
        new ResourcePattern(filter.resourceType(), filter.name(), filter.patternType()) -> acls
    }

    if (resourceToAcl.values.exists(_.isEmpty))
      CommandLineUtils.printUsageAndExit(opts.parser, "You must specify one of: --allow-principal, --deny-principal when trying to add ACLs.")

    resourceToAcl
  }

  private def getResourceFilterToAcls(opts: AclCommandOptions): Map[ResourcePatternFilter, Set[AccessControlEntry]] = {
    var resourceToAcls = Map.empty[ResourcePatternFilter, Set[AccessControlEntry]]

    //if none of the --producer or --consumer options are specified , just construct ACLs from CLI options.
    if (!opts.options.has(opts.producerOpt) && !opts.options.has(opts.consumerOpt)) {
      resourceToAcls ++= getCliResourceFilterToAcls(opts)
    }

    //users are allowed to specify both --producer and --consumer options in a single command.
    if (opts.options.has(opts.producerOpt))
      resourceToAcls ++= getProducerResourceFilterToAcls(opts)

    if (opts.options.has(opts.consumerOpt))
      resourceToAcls ++= getConsumerResourceFilterToAcls(opts).map { case (k, v) => k -> (v ++ resourceToAcls.getOrElse(k, Set.empty[AccessControlEntry])) }

    validateOperation(opts, resourceToAcls)

    resourceToAcls
  }

  private def getProducerResourceFilterToAcls(opts: AclCommandOptions): Map[ResourcePatternFilter, Set[AccessControlEntry]] = {
    val filters = getResourceFilter(opts)

    val topics = filters.filter(_.resourceType == JResourceType.TOPIC)
    val transactionalIds = filters.filter(_.resourceType == JResourceType.TRANSACTIONAL_ID)
    val enableIdempotence = opts.options.has(opts.idempotentOpt)

    val topicAcls = getAcl(opts, Set(WRITE, DESCRIBE, CREATE))
    val transactionalIdAcls = getAcl(opts, Set(WRITE, DESCRIBE))

    //Write, Describe, Create permission on topics, Write, Describe on transactionalIds
    topics.map(_ -> topicAcls).toMap ++
      transactionalIds.map(_ -> transactionalIdAcls).toMap ++
        (if (enableIdempotence)
          Map(ClusterResourceFilter -> getAcl(opts, Set(IDEMPOTENT_WRITE)))
        else
          Map.empty)
  }

  private def getConsumerResourceFilterToAcls(opts: AclCommandOptions): Map[ResourcePatternFilter, Set[AccessControlEntry]] = {
    val filters = getResourceFilter(opts)

    val topics = filters.filter(_.resourceType == JResourceType.TOPIC)
    val groups = filters.filter(_.resourceType == JResourceType.GROUP)

    //Read, Describe on topic, Read on consumerGroup

    val acls = getAcl(opts, Set(READ, DESCRIBE))

    topics.map(_ -> acls).toMap[ResourcePatternFilter, Set[AccessControlEntry]] ++
      groups.map(_ -> getAcl(opts, Set(READ))).toMap[ResourcePatternFilter, Set[AccessControlEntry]]
  }

  private def getCliResourceFilterToAcls(opts: AclCommandOptions): Map[ResourcePatternFilter, Set[AccessControlEntry]] = {
    val acls = getAcl(opts)
    val filters = getResourceFilter(opts)
    filters.map(_ -> acls).toMap
  }

  private def getAcl(opts: AclCommandOptions, operations: Set[AclOperation]): Set[AccessControlEntry] = {
    val allowedPrincipals = getPrincipals(opts, opts.allowPrincipalsOpt)

    val deniedPrincipals = getPrincipals(opts, opts.denyPrincipalsOpt)

    val allowedHosts = getHosts(opts, opts.allowHostsOpt, opts.allowPrincipalsOpt)

    val deniedHosts = getHosts(opts, opts.denyHostsOpt, opts.denyPrincipalsOpt)

    val acls = new collection.mutable.HashSet[AccessControlEntry]
    if (allowedHosts.nonEmpty && allowedPrincipals.nonEmpty)
      acls ++= getAcls(allowedPrincipals, ALLOW, operations, allowedHosts)

    if (deniedHosts.nonEmpty && deniedPrincipals.nonEmpty)
      acls ++= getAcls(deniedPrincipals, DENY, operations, deniedHosts)

    acls.toSet
  }

  private def getAcl(opts: AclCommandOptions): Set[AccessControlEntry] = {
    val operations = opts.options.valuesOf(opts.operationsOpt).asScala
      .map(operation => JSecurityUtils.operation(operation.trim)).toSet
    getAcl(opts, operations)
  }

  def getAcls(principals: Set[KafkaPrincipal], permissionType: AclPermissionType, operations: Set[AclOperation],
              hosts: Set[String]): Set[AccessControlEntry] = {
    for {
      principal <- principals
      operation <- operations
      host <- hosts
    } yield new AccessControlEntry(principal.toString, host, operation, permissionType)
  }

  private def getHosts(opts: AclCommandOptions, hostOptionSpec: OptionSpec[String],
                       principalOptionSpec: OptionSpec[String]): Set[String] = {
    if (opts.options.has(hostOptionSpec))
      opts.options.valuesOf(hostOptionSpec).asScala.map(_.trim).toSet
    else if (opts.options.has(principalOptionSpec))
      Set[String](AclEntry.WildcardHost)
    else
      Set.empty[String]
  }

  private def getPrincipals(opts: AclCommandOptions, principalOptionSpec: OptionSpec[String]): Set[KafkaPrincipal] = {
    if (opts.options.has(principalOptionSpec))
      opts.options.valuesOf(principalOptionSpec).asScala.map(s => JSecurityUtils.parseKafkaPrincipal(s.trim)).toSet
    else
      Set.empty[KafkaPrincipal]
  }

  private def getResourceFilter(opts: AclCommandOptions, dieIfNoResourceFound: Boolean = true): Set[ResourcePatternFilter] = {
    val patternType = opts.options.valueOf(opts.resourcePatternType)

    var resourceFilters = Set.empty[ResourcePatternFilter]
    if (opts.options.has(opts.topicOpt))
      opts.options.valuesOf(opts.topicOpt).forEach(topic => resourceFilters += new ResourcePatternFilter(JResourceType.TOPIC, topic.trim, patternType))

    if (patternType == PatternType.LITERAL && (opts.options.has(opts.clusterOpt) || opts.options.has(opts.idempotentOpt)))
      resourceFilters += ClusterResourceFilter

    if (opts.options.has(opts.groupOpt))
      opts.options.valuesOf(opts.groupOpt).forEach(group => resourceFilters += new ResourcePatternFilter(JResourceType.GROUP, group.trim, patternType))

    if (opts.options.has(opts.transactionalIdOpt))
      opts.options.valuesOf(opts.transactionalIdOpt).forEach(transactionalId =>
        resourceFilters += new ResourcePatternFilter(JResourceType.TRANSACTIONAL_ID, transactionalId, patternType))

    if (opts.options.has(opts.delegationTokenOpt))
      opts.options.valuesOf(opts.delegationTokenOpt).forEach(token => resourceFilters += new ResourcePatternFilter(JResourceType.DELEGATION_TOKEN, token.trim, patternType))

    if (opts.options.has(opts.userPrincipalOpt))
      opts.options.valuesOf(opts.userPrincipalOpt).forEach(user => resourceFilters += new ResourcePatternFilter(JResourceType.USER, user.trim, patternType))

    if (resourceFilters.isEmpty && dieIfNoResourceFound)
      CommandLineUtils.printUsageAndExit(opts.parser, "You must provide at least one resource: --topic <topic> or --cluster or --group <group> or --delegation-token <Delegation Token ID>")

    resourceFilters
  }

  private def confirmAction(opts: AclCommandOptions, msg: String): Boolean = {
    if (opts.options.has(opts.forceOpt))
        return true
    println(msg)
    StdIn.readLine().equalsIgnoreCase("y")
  }

  private def validateOperation(opts: AclCommandOptions, resourceToAcls: Map[ResourcePatternFilter, Set[AccessControlEntry]]): Unit = {
    for ((resource, acls) <- resourceToAcls) {
      val validOps = AclEntry.supportedOperations(resource.resourceType) + AclOperation.ALL
      if ((acls.map(_.operation) -- validOps).nonEmpty)
        CommandLineUtils.printUsageAndExit(opts.parser, s"ResourceType ${resource.resourceType} only supports operations ${validOps.map(JSecurityUtils.operationName).mkString(", ")}")
    }
  }

  class AclCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val CommandConfigDoc = "A property file containing configs to be passed to Admin Client."

    val bootstrapServerOpt: OptionSpec[String] = parser.accepts("bootstrap-server", "A list of host/port pairs to use for establishing the connection to the Kafka cluster." +
      " This list should be in the form host1:port1,host2:port2,... This config is required for acl management using admin client API.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])

    val commandConfigOpt: OptionSpec[String] = parser.accepts("command-config", CommandConfigDoc)
      .withOptionalArg()
      .describedAs("command-config")
      .ofType(classOf[String])

    val authorizerOpt: OptionSpec[String] = parser.accepts("authorizer", "DEPRECATED: Fully qualified class name of " +
      "the authorizer, which defaults to kafka.security.authorizer.AclAuthorizer if --bootstrap-server is not provided. " +
      AclCommand.AuthorizerDeprecationMessage)
      .withRequiredArg
      .describedAs("authorizer")
      .ofType(classOf[String])

    val authorizerPropertiesOpt: OptionSpec[String] = parser.accepts("authorizer-properties", "DEPRECATED: The " +
      "properties required to configure an instance of the Authorizer specified by --authorizer. " +
      "These are key=val pairs. For the default authorizer, example values are: zookeeper.connect=localhost:2181. " +
      AclCommand.AuthorizerDeprecationMessage)
      .withRequiredArg
      .describedAs("authorizer-properties")
      .ofType(classOf[String])

    val topicOpt: OptionSpec[String] = parser.accepts("topic", "topic to which ACLs should be added or removed. " +
      "A value of '*' indicates ACL should apply to all topics.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])

    val clusterOpt: OptionSpecBuilder = parser.accepts("cluster", "Add/Remove cluster ACLs.")
    val groupOpt: OptionSpec[String] = parser.accepts("group", "Consumer Group to which the ACLs should be added or removed. " +
      "A value of '*' indicates the ACLs should apply to all groups.")
      .withRequiredArg
      .describedAs("group")
      .ofType(classOf[String])

    val transactionalIdOpt: OptionSpec[String] = parser.accepts("transactional-id", "The transactionalId to which ACLs should " +
      "be added or removed. A value of '*' indicates the ACLs should apply to all transactionalIds.")
      .withRequiredArg
      .describedAs("transactional-id")
      .ofType(classOf[String])

    val idempotentOpt: OptionSpecBuilder = parser.accepts("idempotent", "Enable idempotence for the producer. This should be " +
      "used in combination with the --producer option. Note that idempotence is enabled automatically if " +
      "the producer is authorized to a particular transactional-id.")

    val delegationTokenOpt: OptionSpec[String] = parser.accepts("delegation-token", "Delegation token to which ACLs should be added or removed. " +
      "A value of '*' indicates ACL should apply to all tokens.")
      .withRequiredArg
      .describedAs("delegation-token")
      .ofType(classOf[String])

    val resourcePatternType: OptionSpec[PatternType] = parser.accepts("resource-pattern-type", "The type of the resource pattern or pattern filter. " +
      "When adding acls, this should be a specific pattern type, e.g. 'literal' or 'prefixed'. " +
      "When listing or removing acls, a specific pattern type can be used to list or remove acls from specific resource patterns, " +
      "or use the filter values of 'any' or 'match', where 'any' will match any pattern type, but will match the resource name exactly, " +
      "where as 'match' will perform pattern matching to list or remove all acls that affect the supplied resource(s). " +
      "WARNING: 'match', when used in combination with the '--remove' switch, should be used with care.")
      .withRequiredArg()
      .ofType(classOf[String])
      .withValuesConvertedBy(new PatternTypeConverter())
      .defaultsTo(PatternType.LITERAL)

    val addOpt: OptionSpecBuilder = parser.accepts("add", "Indicates you are trying to add ACLs.")
    val removeOpt: OptionSpecBuilder = parser.accepts("remove", "Indicates you are trying to remove ACLs.")
    val listOpt: OptionSpecBuilder = parser.accepts("list", "List ACLs for the specified resource, use --topic <topic> or --group <group> or --cluster to specify a resource.")

    val operationsOpt: OptionSpec[String] = parser.accepts("operation", "Operation that is being allowed or denied. Valid operation names are: " + Newline +
      AclEntry.AclOperations.map("\t" + JSecurityUtils.operationName(_)).mkString(Newline) + Newline)
      .withRequiredArg
      .ofType(classOf[String])
      .defaultsTo(JSecurityUtils.operationName(AclOperation.ALL))

    val allowPrincipalsOpt: OptionSpec[String] = parser.accepts("allow-principal", "principal is in principalType:name format." +
      " Note that principalType must be supported by the Authorizer being used." +
      " For example, User:'*' is the wild card indicating all users.")
      .withRequiredArg
      .describedAs("allow-principal")
      .ofType(classOf[String])

    val denyPrincipalsOpt: OptionSpec[String] = parser.accepts("deny-principal", "principal is in principalType:name format. " +
      "By default anyone not added through --allow-principal is denied access. " +
      "You only need to use this option as negation to already allowed set. " +
      "Note that principalType must be supported by the Authorizer being used. " +
      "For example if you wanted to allow access to all users in the system but not test-user you can define an ACL that " +
      "allows access to User:'*' and specify --deny-principal=User:test@EXAMPLE.COM. " +
      "AND PLEASE REMEMBER DENY RULES TAKES PRECEDENCE OVER ALLOW RULES.")
      .withRequiredArg
      .describedAs("deny-principal")
      .ofType(classOf[String])

    val listPrincipalsOpt: OptionSpec[String] = parser.accepts("principal", "List ACLs for the specified principal. principal is in principalType:name format." +
      " Note that principalType must be supported by the Authorizer being used. Multiple --principal option can be passed.")
      .withOptionalArg()
      .describedAs("principal")
      .ofType(classOf[String])

    val allowHostsOpt: OptionSpec[String] = parser.accepts("allow-host", "Host from which principals listed in --allow-principal will have access. " +
      "If you have specified --allow-principal then the default for this option will be set to '*' which allows access from all hosts.")
      .withRequiredArg
      .describedAs("allow-host")
      .ofType(classOf[String])

    val denyHostsOpt: OptionSpec[String] = parser.accepts("deny-host", "Host from which principals listed in --deny-principal will be denied access. " +
      "If you have specified --deny-principal then the default for this option will be set to '*' which denies access from all hosts.")
      .withRequiredArg
      .describedAs("deny-host")
      .ofType(classOf[String])

    val producerOpt: OptionSpecBuilder = parser.accepts("producer", "Convenience option to add/remove ACLs for producer role. " +
      "This will generate ACLs that allows WRITE,DESCRIBE and CREATE on topic.")

    val consumerOpt: OptionSpecBuilder = parser.accepts("consumer", "Convenience option to add/remove ACLs for consumer role. " +
      "This will generate ACLs that allows READ,DESCRIBE on topic and READ on group.")

    val forceOpt: OptionSpecBuilder = parser.accepts("force", "Assume Yes to all queries and do not prompt.")

    val zkTlsConfigFile: OptionSpec[String] = parser.accepts("zk-tls-config-file",
      "DEPRECATED: Identifies the file where ZooKeeper client TLS connectivity properties are defined for" +
        " the default authorizer kafka.security.authorizer.AclAuthorizer." +
        " Any properties other than the following (with or without an \"authorizer.\" prefix) are ignored: " +
        KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.toList.sorted.mkString(", ") +
        ". Note that if SASL is not configured and zookeeper.set.acl is supposed to be true due to mutual certificate authentication being used" +
        " then it is necessary to explicitly specify --authorizer-properties zookeeper.set.acl=true. " +
        AclCommand.AuthorizerDeprecationMessage)
      .withRequiredArg().describedAs("Authorizer ZooKeeper TLS configuration").ofType(classOf[String])

    val userPrincipalOpt: OptionSpec[String] = parser.accepts("user-principal", "Specifies a user principal as a resource in relation with the operation. For instance " +
      "one could grant CreateTokens or DescribeTokens permission on a given user principal.")
      .withRequiredArg()
      .describedAs("user-principal")
      .ofType(classOf[String])

    options = parser.parse(args: _*)

    def checkArgs(): Unit = {
      if (options.has(bootstrapServerOpt) && options.has(authorizerOpt))
        CommandLineUtils.printUsageAndExit(parser, "Only one of --bootstrap-server or --authorizer must be specified")

      if (!options.has(bootstrapServerOpt)) {
        CommandLineUtils.checkRequiredArgs(parser, options, authorizerPropertiesOpt)
        System.err.println(AclCommand.AuthorizerDeprecationMessage)
      }

      if (options.has(commandConfigOpt) && !options.has(bootstrapServerOpt))
        CommandLineUtils.printUsageAndExit(parser, "The --command-config option can only be used with --bootstrap-server option")

      if (options.has(authorizerPropertiesOpt) && options.has(bootstrapServerOpt))
        CommandLineUtils.printUsageAndExit(parser, "The --authorizer-properties option can only be used with --authorizer option")

      val actions = Seq(addOpt, removeOpt, listOpt).count(options.has)
      if (actions != 1)
        CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --list, --add, --remove. ")

      CommandLineUtils.checkInvalidArgs(parser, options, listOpt, producerOpt, consumerOpt, allowHostsOpt, allowPrincipalsOpt, denyHostsOpt, denyPrincipalsOpt)

      //when --producer or --consumer is specified , user should not specify operations as they are inferred and we also disallow --deny-principals and --deny-hosts.
      CommandLineUtils.checkInvalidArgs(parser, options, producerOpt, operationsOpt, denyPrincipalsOpt, denyHostsOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, consumerOpt, operationsOpt, denyPrincipalsOpt, denyHostsOpt)

      if (options.has(listPrincipalsOpt) && !options.has(listOpt))
        CommandLineUtils.printUsageAndExit(parser, "The --principal option is only available if --list is set")

      if (options.has(producerOpt) && !options.has(topicOpt))
        CommandLineUtils.printUsageAndExit(parser, "With --producer you must specify a --topic")

      if (options.has(idempotentOpt) && !options.has(producerOpt))
        CommandLineUtils.printUsageAndExit(parser, "The --idempotent option is only available if --producer is set")

      if (options.has(consumerOpt) && (!options.has(topicOpt) || !options.has(groupOpt) || (!options.has(producerOpt) && (options.has(clusterOpt) || options.has(transactionalIdOpt)))))
        CommandLineUtils.printUsageAndExit(parser, "With --consumer you must specify a --topic and a --group and no --cluster or --transactional-id option should be specified.")
    }
  }
}

class PatternTypeConverter extends EnumConverter[PatternType](classOf[PatternType]) {

  override def convert(value: String): PatternType = {
    val patternType = super.convert(value)
    if (patternType.isUnknown)
      throw new ValueConversionException("Unknown resource-pattern-type: " + value)

    patternType
  }

  override def valuePattern: String = PatternType.values
    .filter(_ != PatternType.UNKNOWN)
    .mkString("|")
}
