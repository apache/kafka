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
import kafka.coordinator.ConsumerCoordinator
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Utils

object AclCommand {

  val delimiter = ","
  val nl = scala.util.Properties.lineSeparator

  def main(args: Array[String]) {

    val opts = new AclCommandOptions(args)

    if (opts.options.has(opts.helpOpt))
      CommandLineUtils.printUsageAndDie(opts.parser, "Usage:")

    opts.checkArgs()

    val authorizerProperties = new Properties()
    if (opts.options.has(opts.authorizerPropertiesOpt)) {
      val props = opts.options.valueOf(opts.authorizerPropertiesOpt).toString.split(delimiter).map(_.split("="))
      props.foreach(pair => authorizerProperties.put(pair(0).trim, pair(1).trim))
    }

    val kafkaConfig = KafkaConfig.fromProps(authorizerProperties)
    val authorizerClass = opts.options.valueOf(opts.authorizerOpt)
    val authZ: Authorizer = CoreUtils.createObject(authorizerClass)
    authZ.configure(kafkaConfig.originals())

    try {
      if (opts.options.has(opts.addOpt))
        addAcl(authZ, opts)
      else if (opts.options.has(opts.removeOpt))
        removeAcl(authZ, opts)
      else if (opts.options.has(opts.listOpt))
        listAcl(authZ, opts)
    } catch {
      case e: Throwable =>
        println(s"Error while executing topic Acl command ${e.getMessage}")
        println(Utils.stackTrace(e))
        System.exit(-1)
    }
  }

  private def addAcl(authZ: Authorizer, opts: AclCommandOptions) {
    val resourceToAcl = getResourceToAcls(opts)

    if (resourceToAcl.values.exists(_.isEmpty))
      CommandLineUtils.printUsageAndDie(opts.parser, "You must specify one of : --allow-principals, --deny-principals when trying to add acls.")

    for ((resource, acls) <- resourceToAcl) {
      val acls = resourceToAcl(resource)
      println(s"Adding following acls for resource: $resource $nl  ${acls.map("\t" + _).mkString(nl)} $nl")
      authZ.addAcls(acls, resource)
    }

    listAcl(authZ, opts)
  }

  private def removeAcl(authZ: Authorizer, opts: AclCommandOptions) {
    val resourceToAcl = getResourceToAcls(opts)

    for ((resource, acls) <- resourceToAcl) {
      if (acls.isEmpty) {
        if (confirmAction(s"Are you sure you want to delete all acls for resource:  $resource y/n?"))
          authZ.removeAcls(resource)
      } else {
        if (confirmAction(s"Are you sure you want to remove acls: $nl ${acls.map("\t" + _).mkString(nl)} $nl from resource $resource y/n?"))
          authZ.removeAcls(acls, resource)
      }
    }

    listAcl(authZ, opts)
  }

  private def listAcl(authZ: Authorizer, opts: AclCommandOptions) {
    val resources = getResource(opts)
    for (resource <- resources) {
      val acls = authZ.getAcls(resource)
      println(s"Following is list of acls for resource : $resource $nl ${acls.map("\t" + _).mkString(nl)} $nl")
    }
  }

  private def getResourceToAcls(opts: AclCommandOptions): Map[Resource, Set[Acl]] = {
    var resourceToAcls = Map.empty[Resource, Set[Acl]]

    //if none of the --producer or --consumer options are specified , just construct acls from CLI options.
    if (!opts.options.has(opts.producerOpt) || !opts.options.has(opts.consumerOpt))
      resourceToAcls ++= getCliResourceToAcls(opts)

    //users are allowed to specify both --producer and --consumer options in a single command.
    if (opts.options.has(opts.producerOpt))
      resourceToAcls ++= getProducerResourceToAcls(opts)

    if (opts.options.has(opts.consumerOpt))
      resourceToAcls ++= getConsumerResourceToAcls(opts).map { case (k, v) => k -> (v ++ resourceToAcls.getOrElse(k, Set.empty[Acl])) }

    resourceToAcls
  }


  private def getProducerResourceToAcls(opts: AclCommandOptions): Map[Resource, Set[Acl]] = {
    val topics: Set[Resource] = getResource(opts).filter(_.resourceType == Topic)

    val acls = getAcl(opts, Set(Write, Describe))

    //Write, Describe permission on topics, Create,Describe permission on cluster
    topics.map(_ -> acls).toMap[Resource, Set[Acl]] +
      (Resource.ClusterResource -> getAcl(opts, Set(Create, Describe)))
  }

  private def getConsumerResourceToAcls(opts: AclCommandOptions): Map[Resource, Set[Acl]] = {
    val resources = getResource(opts)

    val topics: Set[Resource] = getResource(opts).filter(_.resourceType == Topic)
    val consumerGroups: Set[Resource] = resources.filter(_.resourceType == ConsumerGroup)

    //Read,Describe on topic,consumerGroup and ConsumerCoordinator.OffsetsTopicName + Describe,Create on cluster

    val acls = getAcl(opts, Set(Read, Describe))

    topics.map(_ -> acls).toMap[Resource, Set[Acl]] ++
      consumerGroups.map(_ -> acls).toMap[Resource, Set[Acl]] +
      (Resource.ClusterResource -> getAcl(opts, Set(Create, Describe))) +
      (new Resource(Topic, ConsumerCoordinator.OffsetsTopicName) -> acls)
  }

  private def getCliResourceToAcls(opts: AclCommandOptions): Map[Resource, Set[Acl]] = {
    val acls = getAcl(opts)
    val resources = getResource(opts)
    resources.map(_ -> acls).toMap
  }

  private def getAcl(opts: AclCommandOptions, operations: Set[Operation]): Set[Acl] = {
    val allowedPrincipals = getPrincipals(opts, opts.allowPrincipalsOpt)

    val deniedPrincipals = getPrincipals(opts, opts.denyPrincipalsOpt)

    val allowedHosts = getHosts(opts, opts.allowHostsOpt, opts.allowPrincipalsOpt)

    val deniedHosts = getHosts(opts, opts.denyHostssOpt, opts.denyPrincipalsOpt)

    val acls = new collection.mutable.HashSet[Acl]
    if (allowedHosts.nonEmpty && allowedPrincipals.nonEmpty)
      acls ++= getAcls(allowedPrincipals, Allow, operations, allowedHosts)

    if (deniedHosts.nonEmpty && deniedPrincipals.nonEmpty)
      acls ++= getAcls(deniedPrincipals, Deny, operations, deniedHosts)

    acls.toSet
  }

  private def getAcl(opts: AclCommandOptions): Set[Acl] = {
    val operations = opts.options.valueOf(opts.operationsOpt).toString.split(delimiter).map(operation => Operation.fromString(operation.trim)).toSet
    getAcl(opts, operations)
  }

  def getAcls(principals: Set[KafkaPrincipal], permissionType: PermissionType, operations: Set[Operation],
              hosts: Set[String]): Set[Acl] = {
    for {
      principal <- principals
      operation <- operations
      host <- hosts
    } yield new Acl(principal, permissionType, host, operation)
  }

  private def getHosts(opts: AclCommandOptions, hostOptionSpec: ArgumentAcceptingOptionSpec[String],
                       principalOptionSpec: ArgumentAcceptingOptionSpec[String]): Set[String] = {
    if (opts.options.has(hostOptionSpec))
      opts.options.valueOf(hostOptionSpec).toString.split(delimiter).map(_.trim).toSet
    else if (opts.options.has(principalOptionSpec))
      Set[String](Acl.WildCardHost)
    else
      Set.empty[String]
  }

  private def getPrincipals(opts: AclCommandOptions, principalOptionSpec: ArgumentAcceptingOptionSpec[String]): Set[KafkaPrincipal] = {
    if (opts.options.has(principalOptionSpec))
      opts.options.valueOf(principalOptionSpec).toString.split(delimiter).map(s => KafkaPrincipal.fromString(s.trim)).toSet
    else
      Set.empty[KafkaPrincipal]
  }

  private def getResource(opts: AclCommandOptions): Set[Resource] = {
    var resources = Set.empty[Resource]
    if (opts.options.has(opts.topicOpt))
      opts.options.valueOf(opts.topicOpt).toString.split(delimiter).foreach(topic => resources += new Resource(Topic, topic.trim))

    if (opts.options.has(opts.clusterOpt))
      resources += Resource.ClusterResource

    if (opts.options.has(opts.groupOpt))
      opts.options.valueOf(opts.groupOpt).toString.split(delimiter).foreach(consumerGroup => resources += new Resource(ConsumerGroup, consumerGroup.trim))

    if (resources.isEmpty)
      CommandLineUtils.printUsageAndDie(opts.parser, "You must provide at least one resource: --topic <topic> or --cluster or --consumer-group <group>")

    resources
  }

  private def confirmAction(msg: String): Boolean = {
    println(msg)
    Console.readLine().equalsIgnoreCase("y")
  }

  class AclCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val authorizerOpt = parser.accepts("authorizer", "Fully qualified class name of the authorizer, defaults to kafka.security.auth.SimpleAclAuthorizer.")
      .withRequiredArg
      .describedAs("authorizer")
      .ofType(classOf[String])
      .defaultsTo(classOf[SimpleAclAuthorizer].getName)

    val authorizerPropertiesOpt = parser.accepts("authorizer-properties", "REQUIRED:properties required to configure an instance of Authorizer. " +
      "These are comma separated key=val pairs. For the default authorizer the example values are: " +
      "zookeeper.connect=localhost:2181")
      .withRequiredArg
      .describedAs("authorizer-properties")
      .ofType(classOf[String])

    val topicOpt = parser.accepts("topic", "Comma separeted list of topic to which acls should be added or removed.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val clusterOpt = parser.accepts("cluster", "Add/Remove cluster acls.")
    val groupOpt = parser.accepts("consumer-group", "Comma seperated list of consumer groups to which the acls should be added or removed.")
      .withRequiredArg
      .describedAs("consumer-group")
      .ofType(classOf[String])

    val addOpt = parser.accepts("add", "Indicates you are trying to add acls.")
    val removeOpt = parser.accepts("remove", "Indicates you are trying to remove acls.")
    val listOpt = parser.accepts("list", "List acls for the specified resource, use --topic <topic> or --consumer-group <group> or --cluster to specify a resource.")

    val operationsOpt = parser.accepts("operations", "Comma separated list of operations, default is All. Valid operation names are: " + nl +
      Operation.values.map("\t" + _).mkString(nl) + nl)
      .withRequiredArg
      .ofType(classOf[String])
      .defaultsTo(All.name)

    val allowPrincipalsOpt = parser.accepts("allow-principals", "Comma separated list of principals where principal is in principalType:name format." +
      " User:* is the wild card indicating all users.")
      .withRequiredArg
      .describedAs("allow-principals")
      .ofType(classOf[String])

    val denyPrincipalsOpt = parser.accepts("deny-principals", "Comma separated list of principals where principal is in " +
      "principalType: name format. By default anyone not in --allow-principals list is denied access. " +
      "You only need to use this option as negation to already allowed set." +
      "For example if you wanted to allow access to all users in the system but not test-user you can define an acl that " +
      "allows access to user:* and specify --deny-principals=User:test@EXAMPLE.COM. " +
      "AND PLEASE REMEMBER DENY RULES TAKES PRECEDENCE OVER ALLOW RULES.")
      .withRequiredArg
      .describedAs("deny-principals")
      .ofType(classOf[String])

    val allowHostsOpt = parser.accepts("allow-hosts", "Comma separated list of hosts from which principals listed in --allow-principals will have access." +
      "If you have specified --allow-principals then the default for this option will be set to * which allows access from all hosts.")
      .withRequiredArg
      .describedAs("allow-hosts")
      .ofType(classOf[String])

    val denyHostssOpt = parser.accepts("deny-hosts", "Comma separated list of hosts from which principals listed in --deny-principals will be denied access. " +
      "If you have specified --deny-principals then the default for this option will be set to * which denies access from all hosts.")
      .withRequiredArg
      .describedAs("deny-hosts")
      .ofType(classOf[String])

    val producerOpt = parser.accepts("producer", "Convenience option to add/remove acls for producer role.")

    val consumerOpt = parser.accepts("consumer", "Convenience option to add/remove acls for consumer role.")

    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args: _*)

    def checkArgs() {
      CommandLineUtils.checkRequiredArgs(parser, options, authorizerPropertiesOpt)

      val actions = Seq(addOpt, removeOpt, listOpt).count(options.has)
      if (actions != 1)
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --list, --add, --remove.")

      CommandLineUtils.checkInvalidArgs(parser, options, listOpt, Set(producerOpt, consumerOpt, allowHostsOpt, allowPrincipalsOpt, denyHostssOpt, denyPrincipalsOpt))

      //when --producer or --consumer is specified , user should not specify operations as they are inferred and we also disallow --deny-principals and --deny-hosts.
      CommandLineUtils.checkInvalidArgs(parser, options, producerOpt, Set(operationsOpt, denyPrincipalsOpt, denyHostssOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, consumerOpt, Set(operationsOpt, denyPrincipalsOpt, denyHostssOpt))

      if (options.has(producerOpt) && !options.has(topicOpt))
        CommandLineUtils.printUsageAndDie(parser, "With --producer you must specify a --topic")

      if (options.has(consumerOpt) && (!options.has(topicOpt) || !options.has(groupOpt)))
        CommandLineUtils.printUsageAndDie(parser, "With --consumer you must specify a --topic and a --consumer-group")
    }
  }

}
