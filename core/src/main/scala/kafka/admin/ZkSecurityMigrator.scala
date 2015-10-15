package kafka.admin

import javax.security.auth.login.LoginException
import joptsimple.OptionParser
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ToolsUtils, Logging, ZKGroupTopicDirs, ZkUtils, CommandLineUtils}
import org.apache.kafka.common.security.JaasUtils

object ZkSecurityMigrator extends Logging {

  def main(args: Array[String]) {
    val parser = new OptionParser()

    val jaasFileOpt = parser.accepts("jaas.file", "JAAS Config file.").
      withRequiredArg().ofType(classOf[String])
    val zkUrlOpt = parser.accepts("connect", "Sets the ZooKeeper connect string (ensemble). This parameter " +
                                  "takes a comma-separated list of host:port pairs.").
      withRequiredArg().defaultsTo("localhost:2181").ofType(classOf[String])
    val zkSessionTimeoutOpt = parser.accepts("session.timeout", "Sets the ZooKeeper session timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[Integer])
    val zkConnectionTimeoutOpt = parser.accepts("connection.timeout", "Sets the ZooKeeper connection timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[Integer])
    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args : _*)
    if(options.has(helpOpt))
      CommandLineUtils.printUsageAndDie(parser, "ZooKeeper Migration Tool Help")

    if(!options.has(jaasFileOpt) ||
      !JaasUtils.isSecure(System.getProperty(options.valueOf(jaasFileOpt)))) {
      error("No JAAS configuration file has been found. Please make sure that "
            + "you have set the option --jaas.file correctly and that the file"
            + " is valid")
      System.exit(1) 
    }

    val jaasFile = options.valueOf(jaasFileOpt)
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile)
    val zkUrl = options.valueOf(zkUrlOpt)
    val zkSessionTimeout = options.valueOf(zkSessionTimeoutOpt)
    val zkConnectionTimeout = options.valueOf(zkConnectionTimeoutOpt)

    val zkUtils = ZkUtils.create(zkUrl, zkSessionTimeout, zkConnectionTimeout, true)
    for (path <- zkUtils.persistentZkPaths) {
      zkUtils.makeSurePersistentPathExists(path)
    }
  }
}