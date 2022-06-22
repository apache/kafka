/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import joptsimple.{OptionException, OptionSet, OptionSpec}
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Exit, Logging}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, AlterConfigsOptions, ConfigEntry}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.utils.Utils

import java.util
import java.util.{Objects, Properties}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/**
 * This utility class is used to interact with client metrics.  It permits following 3 actions
 * <ul>
 *   <li> add: --add
 *     <ul>[optional] --name human-readable name of the metric subscription. If one is not provided by the user, the admin client will first auto-generate a type-4 UUID to be used as the name before sending the request to the broker.</ul>
 *     <ul> --metric a comma-separated list of metric name prefixes, e.g., "client.producer.partition., client.io.wait". Whitespaces are ignored.</ul>
 *     <ul>[optional] --interval metrics push interval in milliseconds. Defaults to 5 minutes if not specified.</ul>
 *   <li> list: --list
 *     <ul>[optional] --name metric_name.</ul>
 *   <li> delete: --delete
 *     <ul>--name metric_name.</ul>
 * </ul>
 * Essentially, this is a wrapper around {@link org.apache.kafka.clients.admin.AdminClient}.  {@code --list} performs
 * the {@code describeConfigs} operation, {@code --add} and {@code --delete} performs the {@code alterConfigs} operation.
 *
 */
// TODO: verify if --match selector works on broker side
object ClientMetrics extends Logging {
  val ADMIN_CLIENT_TIMEOUT = 10000
  def main(args: Array[String]): Unit = {
    try {
      val opts = new ConfigCommandParser(args)
      CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool helps to manipulate and describe entity config for a topic, client, user, broker or ip")
      opts.checkArgs()

      processCommand(opts)
    } catch {
      case e@(_: IllegalArgumentException | _: InvalidConfigurationException | _: OptionException) =>
        logger.debug(s"Failed config command with args '${args.mkString(" ")}'", e)
        System.err.println(e.getMessage)
        Exit.exit(1)

      case t: Throwable =>
        logger.debug(s"Error while executing config command with args '${args.mkString(" ")}'", t)
        System.err.println(s"Error while executing config command with args '${args.mkString(" ")}'")
        t.printStackTrace(System.err)
        Exit.exit(1)
    }
  }

  private def processCommand(opts: ConfigCommandParser): Unit = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
    val clientMetricService = ClientMetricService(props, opts.bootstrapServer)

    var exitCode = 0
    try {
      if (opts.options.has(opts.addOp))
        clientMetricService.addMetrics(new AddMetricsOptions(opts))
      else if (opts.options.has(opts.listOp)) {
        clientMetricService.listMetrics(new ListMetricsOptions(opts))
      } else if (opts.options.has(opts.deleteOp))
        clientMetricService.deleteMetrics(new DeleteMetricsOptions(opts))
      else
        throw new IllegalArgumentException("Must be one of the action: add, list, delete")
    } catch {
      case e: Throwable =>
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      clientMetricService.close()
      Exit.exit(exitCode)
    }
  }

  class ConfigCommandParser(args: Array[String]) extends CommandDefaultOptions(args) {
    def has(builder: OptionSpec[_]): Boolean = options.has(builder)
    def valueAsOption[A](option: OptionSpec[A], defaultValue: Option[A] = None): Option[A] = if (has(option)) Some(options.valueOf(option)) else defaultValue
    def valuesAsOption[A](option: OptionSpec[A], defaultValue: Option[util.List[A]] = None): Option[util.List[A]] = if (has(option)) Some(options.valuesOf(option)) else defaultValue

    val addOp = parser.accepts("add", "Adding telemetry metrics")
    val deleteOp = parser.accepts("delete", "Delete metrics")
    val listOp = parser.accepts("list", "List metrics")

    // TODO: verify if it is working on broker
    val matchSelector = parser.accepts("match", "client matching selector")
      .withRequiredArg
      .describedAs("matching a specific client ID")
      .ofType(classOf[String])
    val metric = parser.accepts("metric", "metric prefixes")
      .withRequiredArg
      .ofType(classOf[String])
      .withValuesSeparatedBy(",")

    val interval = parser.accepts("interval", "push interval ms")
      .withRequiredArg
      .ofType(classOf[Long])
    val block = parser.accepts("block", "blocking metrics collection")
    val name = parser.accepts("name", "metric name")
      .withRequiredArg()
      .describedAs("config name")
      .ofType(classOf[String])

    val bootstrapServerOpt = parser.accepts("bootstrap-server", "The Kafka server to connect to. " +
      "This is required for describing and altering broker configs.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])

    val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client. " +
      "This is used only with --bootstrap-server option for describing and altering broker configs.")
      .withRequiredArg
      .describedAs("command config property file")
      .ofType(classOf[String])

    options = parser.parse(args : _*)

    def bootstrapServer = valueAsOption(bootstrapServerOpt)

    def checkArgs(): Unit = {
      // only perform 1 action at a time
      val actions = Seq(addOp, deleteOp, listOp).count(options.has _)
      if (actions != 1)
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --add, --delete, --list")

      // check add args
      CommandLineUtils.checkInvalidArgs(parser, options, addOp, Set(deleteOp, listOp))

      // check delete args
      CommandLineUtils.checkInvalidArgs(parser, options, deleteOp, Set(addOp, listOp, matchSelector, metric, interval))

      // check list args
      CommandLineUtils.checkInvalidArgs(parser, options, listOp, Set(addOp, deleteOp, matchSelector, metric, interval, block))

      if (!options.has(bootstrapServerOpt))
        throw new IllegalArgumentException("--bootstrap-server must be specified")
    }
  }

  object ClientMetricService {
    def createAdminClient(commandConfig: Properties, bootstrapServer: Option[String]): Admin = {
      bootstrapServer match {
        case Some(serverList) => commandConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, serverList)
        case None =>
      }
      Admin.create(commandConfig)
    }


    def apply(commandConfig: Properties, bootstrapServer: Option[String]): ClientMetricService =
      ClientMetricService(createAdminClient(commandConfig, bootstrapServer))

    case class ClientMetricService private (adminClient: Admin) extends AutoCloseable {
      def listMetrics(listMetricsOpts: ListMetricsOptions): Unit = {
        val configResource = if(listMetricsOpts.name.isEmpty) {
          // TODO: How do we get all metrics? Not sure if this is legal
          new ConfigResource(ConfigResource.Type.CLIENT_METRICS, null)
        } else
          new ConfigResource(ConfigResource.Type.CLIENT_METRICS, listMetricsOpts.name.get)

        adminClient.describeConfigs(util.Collections.singleton(configResource))
      }

      def deleteMetrics(deleteMetricsOpts: DeleteMetricsOptions): Unit = {
        if(deleteMetricsOpts.name.isEmpty)
          throw new IllegalArgumentException(s"The delete metrics operation requires a name")

        val configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, deleteMetricsOpts.name.get)
        val alterOptions = new AlterConfigsOptions().timeoutMs(ADMIN_CLIENT_TIMEOUT).validateOnly(false)
        val alterEntries = List(new AlterConfigOp(new ConfigEntry("name", deleteMetricsOpts.name.get), AlterConfigOp.OpType.DELETE))
        adminClient.incrementalAlterConfigs(
          Map(configResource -> alterEntries.asJavaCollection).asJava,
          alterOptions).all().get(60, TimeUnit.SECONDS)
      }

      def addMetrics(addMetricsOpts: AddMetricsOptions): Unit = {
        val configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, addMetricsOpts.name.get)
        val alterOptions = new AlterConfigsOptions().timeoutMs(ADMIN_CLIENT_TIMEOUT).validateOnly(false)
        val configsToBeAdded = addMetricsOpts.getAddedConfig
        val alterEntries = (configsToBeAdded.map( e => new AlterConfigOp(new ConfigEntry(e._1, e._2), AlterConfigOp.OpType.SET))).asJavaCollection
        adminClient.incrementalAlterConfigs(
          Map(configResource -> alterEntries).asJava,
          alterOptions).all().get(60, TimeUnit.SECONDS)
      }

      override def close(): Unit = adminClient.close()
    }
  }

  class ListMetricsOptions(args: ConfigCommandParser) extends MetricsOptions(args.options: OptionSet) {
    def name: Option[String] = valueAsOption(args.name)

    override def hashCode(): Int = Objects.hash(options, args)

    override def equals(obj: Any): Boolean = {
      if (obj == null)
        return false

      if (this.getClass != obj.getClass)
        return false

      val that = obj.asInstanceOf[ListMetricsOptions]
      options.equals(that.options)
    }
  }

  class DeleteMetricsOptions(args: ConfigCommandParser) extends MetricsOptions(args.options: OptionSet) {
    def name: Option[String] = valueAsOption(args.name)

    override def hashCode(): Int = Objects.hash(options, args)

    override def equals(obj: Any): Boolean = {
      if (obj == null)
        return false

      if (this.getClass != obj.getClass)
        return false

      val that = obj.asInstanceOf[DeleteMetricsOptions]
      options.equals(that.options)
    }
  }

  class AddMetricsOptions(args: ConfigCommandParser) extends MetricsOptions(args.options: OptionSet) {
    def name: Option[String] = valueAsOption(args.name)
    def metrics: Option[util.List[String]] = valuesAsOption(args.metric)
    def intervalMs: Option[Long] = valueAsOption(args.interval)
    def isBlocked: Boolean = has(args.block)

    def getAddedConfig: Map[String, String] = {
     var props: Map[String, String] = Map()

      if(name.isDefined) props += ("name" -> name.get)
      if(metrics.isDefined) props += ("metrics" -> metrics.get.toString)
      if(intervalMs.isDefined) props += ("interval" -> intervalMs.get.toString)
      if(isBlocked) props += ("block" -> isBlocked.toString)
      props
    }

    override def hashCode(): Int = Objects.hash(options, args)

    override def equals(obj: Any): Boolean = {
      if (obj == null)
        return false

      if (this.getClass != obj.getClass)
        return false

      val that = obj.asInstanceOf[AddMetricsOptions]
      options.equals(that.options)
    }
  }

  case class MetricsOptions(options: OptionSet) {
    def has(builder: OptionSpec[_]): Boolean = options.has(builder)
    def valueAsOption[A](option: OptionSpec[A], defaultValue: Option[A] = None): Option[A] = if (has(option)) Some(options.valueOf(option)) else defaultValue
    def valuesAsOption[A](option: OptionSpec[A], defaultValue: Option[util.List[A]] = None): Option[util.List[A]] = if (has(option)) Some(options.valuesOf(option)) else defaultValue
  }
}
