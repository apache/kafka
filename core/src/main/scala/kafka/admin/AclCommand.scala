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

import java.io.{File, FileInputStream}
import java.util.Properties

import joptsimple._
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Utils

object AclCommand {

  val delimeter = ","
  val nl = scala.util.Properties.lineSeparator

  def main(args: Array[String]): Unit = {

    val opts = new AclCommandOptions(args)

    opts.checkArgs()

    val actions = Seq(opts.addOpt, opts.removeOpt, opts.listOpt).count(opts.options.has _)
    if (actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --add, --remove.")

    val props = new Properties()
    props.load(new FileInputStream(new File(opts.options.valueOf(opts.config))))

    if (!props.containsKey(KafkaConfig.AuthorizerClassNameProp))
      CommandLineUtils.printUsageAndDie(opts.parser, "Config file must set property = " + KafkaConfig.AuthorizerClassNameProp
        + " to a class that implements Authorizer interface.")

    val kafkaConfig = KafkaConfig.fromProps(props)
    val authZ: Authorizer = CoreUtils.createObject(kafkaConfig.authorizerClassName)
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
        println("Error while executing topic command " + e.getMessage)
        println(Utils.stackTrace(e))
    }
  }

  private def addAcl(authZ: Authorizer, opts: AclCommandOptions): Unit = {
    val acls: Set[Acl] = getAcl(opts)

    if (acls.isEmpty)
      CommandLineUtils.printUsageAndDie(opts.parser, "You must specify one of : --allowprincipals, --denyPrincipals when trying to add acls.")

    val resource = getResource(opts)

    println("Adding acls: " + nl + acls.map("\t" + _).mkString(nl) + nl)
    authZ.addAcls(acls, resource)

    listAcl(authZ, opts)
  }

  private def removeAcl(authZ: Authorizer, opts: AclCommandOptions): Unit = {
    val acls: Set[Acl] = getAcl(opts)
    val resource = getResource(opts)
    if (acls.isEmpty) {
      if (confirmaAction("Are you sure you want to delete all acls for resource: " + getResource(opts) + " y/n?"))
        authZ.removeAcls(resource)
    } else {
      if (confirmaAction(("Are you sure you want to remove acls: " + nl + acls.map("\t" + _).mkString(nl) + nl) + "  from resource " + getResource(opts) + " y/n?"))
        authZ.removeAcls(acls, resource)
    }

    listAcl(authZ, opts)
  }

  private def listAcl(authZ: Authorizer, opts: AclCommandOptions): Unit = {
    val acls: Set[Acl] = authZ.getAcls(getResource(opts))
    println("Following is list of  acls for resource : " + getResource(opts) + nl + acls.map("\t" + _).mkString(nl) + nl)
  }

  private def getAcl(opts: AclCommandOptions): Set[Acl] = {
    val allowedPrincipals = getPrincipals(opts, opts.allowPrincipalsOpt)

    val deniedPrincipals = getPrincipals(opts, opts.denyPrincipalsOpt)

    val allowedHosts = getHosts(opts, opts.allowHostsOpt, opts.allowPrincipalsOpt)

    val deniedHosts = getHosts(opts, opts.denyHostssOpt, opts.denyPrincipalsOpt)

    val operations = if (opts.options.has(opts.operationsOpt))
      opts.options.valueOf(opts.operationsOpt).toString.split(delimeter).map(s => Operation.fromString(s)).toSet
    else
      Set[Operation](All)

    var acls = new collection.mutable.HashSet[Acl]
    if (allowedHosts.nonEmpty && allowedPrincipals.nonEmpty)
      acls ++= getAcls(allowedPrincipals, Allow, operations, allowedHosts)

    if (deniedHosts.nonEmpty && deniedPrincipals.nonEmpty)
      acls ++= getAcls(deniedPrincipals, Deny, operations, deniedHosts)

    acls.toSet
  }

  def getAcls(principals: Set[KafkaPrincipal], permissionType: PermissionType, operations: Set[Operation],
              hosts: Set[String]): Set[Acl] = {
    var acls = new collection.mutable.HashSet[Acl]
    for {
      principal <- principals
      operation <- operations
      host <- hosts
    } acls += new Acl(principal, permissionType, host, operation)

    acls.toSet
  }

  private def getHosts(opts: AclCommandOptions, hostOptionSpec: ArgumentAcceptingOptionSpec[String],
                       principalOptionSpec: ArgumentAcceptingOptionSpec[String]): Set[String] = {
    if (opts.options.has(hostOptionSpec))
      opts.options.valueOf(hostOptionSpec).toString.split(delimeter).toSet
    else if (opts.options.has(principalOptionSpec))
      Set[String](Acl.WildCardHost)
    else
      Set.empty[String]
  }

  private def getPrincipals(opts: AclCommandOptions, principalOptionSpec: ArgumentAcceptingOptionSpec[String]): Set[KafkaPrincipal] = {
    if (opts.options.has(principalOptionSpec))
      opts.options.valueOf(principalOptionSpec).toString.split(delimeter).map(s => KafkaPrincipal.fromString(s)).toSet
    else
      Set.empty[KafkaPrincipal]
  }

  private def getResource(opts: AclCommandOptions): Resource = {
    val resource: Resource = if (opts.options.has(opts.topicOpt))
      new Resource(Topic, opts.options.valueOf(opts.topicOpt).toString)
    else if (opts.options.has(opts.clusterOpt))
      Resource.ClusterResource
    else if (opts.options.has(opts.groupOpt))
      new Resource(ConsumerGroup, opts.options.valueOf(opts.groupOpt).toString)
    else
      null

    if (resource == null)
      CommandLineUtils.printUsageAndDie(opts.parser, "You must provide at least one resource: --topic <topic> or --cluster or --consumer-group <group>")

    resource
  }

  private def confirmaAction(msg: String): Boolean = {
    println(msg)
    val userInput: String = Console.readLine()

    "y".equalsIgnoreCase(userInput)
  }

  class AclCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val config = parser.accepts("config", "REQUIRED: Path to server.properties file.")
      .withRequiredArg
      .describedAs("config")
      .ofType(classOf[String])

    val topicOpt = parser.accepts("topic", "Topic to which acls should be added or removed.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val clusterOpt = parser.accepts("cluster", "Add/Remove cluster acls.")
    val groupOpt = parser.accepts("consumer-group", "Add remove acls for a consumer-group.")
      .withRequiredArg
      .describedAs("consumer-group")
      .ofType(classOf[String])

    val addOpt = parser.accepts("add", "Indicates you are trying to add acls.")
    val removeOpt = parser.accepts("remove", "Indicates you are trying to remove acls.")
    val listOpt = parser.accepts("list", "List acls for the specified resource, use --topic <topic> or --consumer-group <group> or --cluster.")

    val operationsOpt = parser.accepts("operations", "Comma separated list of operations, default is All. Valid operation names are: " + nl +
      Operation.values.map("\t" + _).mkString(nl) + nl)
      .withRequiredArg
      .ofType(classOf[String])

    val allowPrincipalsOpt = parser.accepts("allowprincipals", "Comma separated list of principals where principal is in principalType:name format." +
      " User:* is the wild card indicating all users.")
      .withRequiredArg
      .describedAs("allowprincipals")
      .ofType(classOf[String])

    val denyPrincipalsOpt = parser.accepts("denyprincipals", "Comma separated list of principals where principal is in principalType: name format. By default anyone not in --allowprincipals list is denied access. " +
      "You only need to use this option as negation to already allowed set." +
      "For example if you wanted to allow access to all users in the system but not test-user you can define an acl that allows access to user:* and specify --denyprincipals=User:test@EXAMPLE.COM. " +
      "AND PLEASE REMEMBER DENY RULES TAKES PRECEDENCE OVER ALLOW RULES.")
      .withRequiredArg
      .describedAs("denyPrincipals")
      .ofType(classOf[String])

    val allowHostsOpt = parser.accepts("allowhosts", "Comma separated list of hosts from which principals listed in --allowprincipals will have access." +
      "If you have specified --allowprincipals then the default for this option will be set to * which allows access from all hosts.")
      .withRequiredArg
      .describedAs("allowhosts")
      .ofType(classOf[String])

    val denyHostssOpt = parser.accepts("denyhosts", "Comma separated list of hosts from which principals listed in --denyprincipals will be denied access. " +
      "If you have specified --denyprincipals then the default for this option will be set to * which denies access from all hosts.")
      .withRequiredArg
      .describedAs("denyhosts")
      .ofType(classOf[String])

    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args: _*)

    def checkArgs() {
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, config)
    }
  }

}