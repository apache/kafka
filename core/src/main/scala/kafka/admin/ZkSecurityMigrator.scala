package kafka.admin

import joptsimple.OptionParser
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ToolsUtils, Logging, ZKGroupTopicDirs, ZkUtils, CommandLineUtils}

object ZkSecurityMigrator extends Logging {

  def main(args: Array[String]) {
    // Make sure that the JAAS file has been appropriately set
    if(!ToolsUtils.isSecure(System.getProperty("java.security.auth.login.config"))) {
      warn("No JAAS configuration file has been found. Please make sure that "
            + "you have set the system property %s correctly and that the file"
            + " is valid".format("java.security.auth.login.config"))
      System.exit(0);
    }

    val parser = new OptionParser()

    val zkUrlOpt = parser.accepts("zookeeper.connect", "ZooKeeper connect string.").
      withRequiredArg().defaultsTo("localhost:2181").ofType(classOf[String])
    val zkSessionTimeoutOpt = parser.accepts("zookeeper.session.timeout", "ZooKeeper session timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[Int])
    val zkConnectionTimeoutOpt = parser.accepts("zookeeper.connection.timeout", "ZooKeeper session timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[Int])
    parser.accepts("help", "Print this message.")

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Validate that all partitions have a consumer for a given consumer group.")

    val options = parser.parse(args : _*)
    val zkUrl = options.valueOf(zkUrlOpt)
    val zkSessionTimeout = options.valueOf(zkSessionTimeoutOpt)
    val zkConnectionTimeout = options.valueOf(zkConnectionTimeoutOpt)

    val zkUtils = ZkUtils.create(zkUrl, zkSessionTimeout, zkConnectionTimeout, true)
    for (path <- zkUtils.persistentZkPaths) {
      zkUtils.makeSurePersistentPathExists(path)
    }
  }
}