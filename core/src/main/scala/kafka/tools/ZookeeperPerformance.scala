package kafka.tools

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}

import joptsimple.OptionParser
import kafka.utils.CommandLineUtils
import org.apache.zookeeper.AsyncCallback.{DataCallback, MultiCallback, StatCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, KeeperException, Op, OpResult, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

object ZookeeperPerformance {
  def main(args: Array[String]): Unit = {
    val opts = new ZookeeperPerformanceCommandOptions(args)
    opts.checkArgs()
    val count = opts.options.valueOf(opts.countOpt)
    val zookeeperPerformance = new ZookeeperPerformance(opts.options.valueOf(opts.zkConnectOpt))
    if (opts.options.has(opts.createOpt)) {
      test(() => zookeeperPerformance.create(count))
    } else if (opts.options.has(opts.deleteOpt)) {
      test(() => zookeeperPerformance.delete(count))
    } else if (opts.options.has(opts.getDataSyncOpt)) {
      test(() => zookeeperPerformance.getDataSync(count))
    } else if (opts.options.has(opts.getDataAsyncOpt)) {
      test(() => zookeeperPerformance.getDataAsync(count))
    } else if (opts.options.has(opts.setDataSyncOpt)) {
      test(() => zookeeperPerformance.setDataSync(count))
    } else if (opts.options.has(opts.setDataAsyncOpt)) {
      test(() => zookeeperPerformance.setDataAsync(count))
    } else if (opts.options.has(opts.multiSetDataSyncOpt)) {
      test(() => zookeeperPerformance.multiSetDataSync(count, opts.options.valueOf(opts.batchSizeOpt)))
    } else if (opts.options.has(opts.multiCheckAndSetDataAsyncOpt)) {
      test(() => zookeeperPerformance.multiCheckAndSetDataAsync(count, opts.options.valueOf(opts.batchSizeOpt)))
    }
    zookeeperPerformance.close()
  }

  private def time(f: () => Unit): (Long, Option[Exception]) = {
    val start = System.currentTimeMillis()
    try {
      f()
      val end = System.currentTimeMillis()
      (end - start, None)
    } catch {
      case e: Exception =>
        val end = System.currentTimeMillis()
        (end - start, Option(e))
    }
  }

  private def test(f: () => Unit): Unit = {
    val (duration, exceptionOpt) = time(f)
    if (exceptionOpt.isEmpty) println(s"Succeeded after $duration ms")
    else println(s"Failed after $duration ms with exception: ${exceptionOpt.get}")
  }

  class ZookeeperPerformanceCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val helpOpt = parser.accepts("help", "Print usage information.")
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .ofType(classOf[String])
    val createOpt = parser.accepts("create", "Create znodes")
    val deleteOpt = parser.accepts("delete", "Delete znodes")
    val getDataSyncOpt = parser.accepts("get-data-sync", "Read znodes synchronously")
    val getDataAsyncOpt = parser.accepts("get-data-async", "Read znodes asynchronously")
    val setDataSyncOpt = parser.accepts("set-data-sync", "Write znodes synchronously")
    val setDataAsyncOpt = parser.accepts("set-data-async", "Write znodes asynchronously")
    val multiSetDataSyncOpt = parser.accepts("multi-set-data-sync", "Write atomic batches of znodes synchronously")
    val multiCheckAndSetDataAsyncOpt = parser.accepts("multi-check-and-set-data-async", "Atomically check a znode and write a batch of znodes asynchronously")
    val countOpt = parser.accepts("count", "REQUIRED: The number of znodes")
      .withRequiredArg
      .ofType(classOf[Int])
    val batchSizeOpt = parser.accepts("batch-size", "The number of writes to batch together")
      .withRequiredArg
      .ofType(classOf[Int])

    val options = parser.parse(args : _*)
    val actions = Set(createOpt, deleteOpt, getDataSyncOpt, getDataAsyncOpt, setDataSyncOpt, setDataAsyncOpt, multiSetDataSyncOpt, multiCheckAndSetDataAsyncOpt)
    val actionsRequiringBatchSize = Set(multiSetDataSyncOpt, multiCheckAndSetDataAsyncOpt)

    def checkArgs(): Unit = {
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt, countOpt)
      if (actions.count(options.has) != 1) {
        CommandLineUtils.printUsageAndDie(parser, s"Command must include exactly one action: $actions")
      }
      if (actionsRequiringBatchSize.filter(options.has).nonEmpty) {
        CommandLineUtils.checkRequiredArgs(parser, options, batchSizeOpt)
      }
      CommandLineUtils.checkInvalidArgs(parser, options, createOpt, Set(batchSizeOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, deleteOpt, Set(batchSizeOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, getDataSyncOpt, Set(batchSizeOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, getDataAsyncOpt, Set(batchSizeOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, setDataSyncOpt, Set(batchSizeOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, setDataAsyncOpt, Set(batchSizeOpt))
    }
  }
}

class ZookeeperPerformance(zkConnect: String) extends Watcher {
  private val zookeeper = new ZooKeeper(zkConnect, 30000, this)

  private def bytes = UUID.randomUUID().toString.getBytes(StandardCharsets.UTF_8)
  private def path(x: Int) = s"/$x"

  private val checkPath = path(Int.MaxValue)

  def create(count: Int): Unit = {
    (1 to count).foreach { x =>
      try {
        zookeeper.create(path(x), Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      } catch {
        case e: KeeperException.NodeExistsException =>
      }
    }
    try {
      zookeeper.create(checkPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    } catch {
      case e: KeeperException.NodeExistsException =>
    }
  }

  def delete(count: Int): Unit = {
    (1 to count).foreach { x =>
      try {
        zookeeper.delete(path(x), -1)
      } catch {
        case e: KeeperException.NoNodeException =>
      }
    }
    try {
      zookeeper.delete(checkPath, -1)
    } catch {
      case e: KeeperException.NoNodeException =>
    }
  }

  def getDataSync(count: Int): Unit = {
    (1 to count).foreach(x => zookeeper.getData(path(x), false, null))
  }

  def getDataAsync(count: Int): Unit = {
    val countDownLatch = new CountDownLatch(count)
    val errorCodes = new LinkedBlockingQueue[Code]()
    (1 to count).foreach(x => zookeeper.getData(path(x), false, new DataCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat) = {
        val code = Code.get(rc)
        if (code != Code.OK) errorCodes.add(code)
        countDownLatch.countDown()
      }
    }, null))
    countDownLatch.await()
    if (!errorCodes.isEmpty) throw new RuntimeException(s"error codes: $errorCodes")
  }

  def setDataSync(count: Int): Unit = {
    val data = bytes
    (1 to count).foreach(x => zookeeper.setData(path(x), data, -1))
  }

  def setDataAsync(count: Int): Unit = {
    val countDownLatch = new CountDownLatch(count)
    val data = bytes
    val errorCodes = new LinkedBlockingQueue[Code]()
    (1 to count).foreach(x => zookeeper.setData(path(x), data, -1, new StatCallback {
      override def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat): Unit = {
        val code = Code.get(rc)
        if (code != Code.OK) errorCodes.add(code)
        countDownLatch.countDown()
      }
    }, null))
    countDownLatch.await()
    if (!errorCodes.isEmpty) throw new RuntimeException(s"error codes: $errorCodes")
  }

  def multiSetDataSync(count: Int, batchSize: Int): Unit = {
    val data = bytes
    val ops = (1 to count).map(x => Op.setData(path(x), data, -1)).toArray
    val batches = ops.grouped(batchSize).map { batch =>
      val list = new java.util.ArrayList[Op]()
      batch.foreach(list.add)
      list
    }
    batches.foreach(zookeeper.multi)
  }

  def multiCheckAndSetDataAsync(count: Int, batchSize: Int): Unit = {
    val data = bytes
    val checkStat = new Stat()
    zookeeper.getData(checkPath, false, checkStat)
    val ops = (1 to count).map(x => Op.setData(path(x), data, -1)).toArray
    val batches = ops.grouped(batchSize).map { batch =>
      val list = new java.util.ArrayList[Op]()
      list.add(Op.check(checkPath, checkStat.getVersion))
      batch.foreach(list.add)
      list
    }.toList
    val countDownLatch = new CountDownLatch(batches.size)
    val errorCodes = new LinkedBlockingQueue[Code]()
    batches.foreach { batch =>
      zookeeper.multi(batch, new MultiCallback {
        override def processResult(rc: Int, path: String, ctx: scala.Any, opResults: java.util.List[OpResult]) = {
          val code = Code.get(rc)
          if (code != Code.OK) errorCodes.add(code)
          countDownLatch.countDown()
        }
      }, null)
    }
    countDownLatch.await()
    if (!errorCodes.isEmpty) throw new RuntimeException(s"error codes: $errorCodes")
  }

  def close(): Unit = {
    zookeeper.close()
  }

  override def process(event: WatchedEvent): Unit = {}
}