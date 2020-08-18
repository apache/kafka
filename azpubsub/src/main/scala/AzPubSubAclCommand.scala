import kafka.admin
import kafka.admin.AclCommand
import kafka.admin.AclCommand.{AclCommandOptions, AdminClientService, AuthorizerService, JAuthorizerService, Newline, getPrincipals, getResourceFilter}
import kafka.security.auth.{Authorizer, SimpleAclAuthorizer}
import kafka.utils.{CommandLineUtils, Exit, Json, Logging}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.acl
import org.apache.kafka.common.acl.{AccessControlEntry, AclOperation, AclPermissionType}

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.collection.mutable
import scala.io.StdIn

object AzPubSubAclCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val opts = new AzPubSubAclCommandOptions(args)

    CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool helps to manage acls on kafka.")

    opts.checkArgs()

    val aclCommandService = new AzPubSubAdminClientService(opts)

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

  def GetProducerAclOperations(): Set[AclOperation] = {
    var dummyArgs = Array[String]("--bootstrap-server", "localhost:9092", "--add", "--allow-principal", "User:Bob", "--producer", "--topic", "Test-topic")
    var dummyOpt = new AclCommandOptions(dummyArgs)
    var resourceMap = AclCommand.getProducerResourceFilterToAcls(dummyOpt)
    for ((key, value) <- resourceMap) {
      if (key.resourceType() == ResourceType.TOPIC) {
        var aclOperationList = Set[AclOperation]()
        value.foreach(acl => aclOperationList += acl.operation())
        return aclOperationList
      }
    }
    return Set[AclOperation]()
  }

  def GetConsumerAclOperations(): Set[AclOperation] = {
    var dummyArgs = Array[String]("--bootstrap-server", "localhost:9092", "--add", "--allow-principal", "User:Bob", "--consumer", "--topic", "Test-topic", "--group", "Test-group")
    var dummyOpt = new AclCommandOptions(dummyArgs)
    var resourceMap = AclCommand.getConsumerResourceFilterToAcls(dummyOpt)
    for ((key, value) <- resourceMap) {
      if (key.resourceType() == ResourceType.TOPIC) {
        var aclOperationList = Set[AclOperation]()
        value.foreach(acl => aclOperationList += acl.operation())
        return aclOperationList
      }
    }
    return Set[AclOperation]()
  }

  def GetGroupAclOperations(): Set[AclOperation] = {
    var dummyArgs = Array[String]("--bootstrap-server", "localhost:9092", "--add", "--allow-principal", "User:Bob", "--consumer", "--topic", "Test-topic", "--group", "Test-group")
    var dummyOpt = new AclCommandOptions(dummyArgs)
    var resourceMap = AclCommand.getConsumerResourceFilterToAcls(dummyOpt)
    for ((key, value) <- resourceMap) {
      if (key.resourceType() == ResourceType.GROUP) {
        var aclOperationList = Set[AclOperation]()
        value.foreach(acl => aclOperationList += acl.operation())
        return aclOperationList
      }
    }
    return Set[AclOperation]()
  }

  def AclToProducerConsumerMapping(resourceToAcls:Map[ResourcePattern,Set[AccessControlEntry]]):mutable.Map[ResourcePattern,mutable.Set[AzPubSubAccessControlEntry]] = {
    //call list here or something else get existing acls  (do this)
    //inside one resource use principal filter? if faster
    // iterate through principal and see if principal matching happens (do this)
    // Add corresponding principal acl and consumer acls (do it in one pass) bool true if all 3 are present (do this)
    //host has to be * and allowprincipal true---record this // (dont count these) or create list of bool of each req (do this)
    //count for number of acls that have matched with this acl operation list of producer and consumer (do this)
    //create the data structure with modified acls (try to create same as list structure)
    //figure out if current jsonify function is usable or need to extend
    //jsonify this new ds (pull out jsonify if needed)
    //create new flag to output this ds with list? (output modified + jsonified for this new flag) (do this -> add similar flag as json)
    //override list with this stuff
    //think about similar override for add and remove
    //convert existing acls to new type of acls - existing acl should be new type of acl
    //assume everything is either a topic or group

    var producerConsumerAclMap = mutable.Map[ResourcePattern, mutable.Set[AzPubSubAccessControlEntry]]()
    var producerAclOperations = GetProducerAclOperations()
    var consumerAclOperations = GetConsumerAclOperations()

    resourceToAcls.foreach(resource => {
      producerConsumerAclMap += (resource._1 -> mutable.Set[AzPubSubAccessControlEntry]())
      if(resource._1.resourceType() == ResourceType.TOPIC){
        var principalAclMap = mutable.Map[String,mutable.Set[AccessControlEntry]]()
        resource._2.foreach(acl => {
          if (principalAclMap.contains(acl.principal())) {
            principalAclMap(acl.principal()).add(acl)
          }
          else{
          principalAclMap += (acl.principal() -> mutable.Set(acl))
          }
        })
        principalAclMap.foreach { case (principal, acls) => {
          var strayAcls = acls
          var filteredAclOperations = mutable.Set[AclOperation]()
          var filteredAcls = acls.filter(x => (x.host() == "*" && x.permissionType() == AclPermissionType.ALLOW))
          filteredAcls.foreach(x => filteredAclOperations.add(x.operation()))
          if(producerAclOperations.subsetOf(filteredAclOperations)){
            strayAcls = strayAcls.filterNot(x => (x.host() == "*" && x.permissionType() == AclPermissionType.ALLOW && producerAclOperations.contains(x.operation())))
            var modifiedAcl = new AzPubSubAccessControlEntry(principal, "*", AclOperation.ANY, AclPermissionType.ALLOW, "PRODUCER")
            producerConsumerAclMap(resource._1).add(modifiedAcl)
          }

          if(consumerAclOperations.subsetOf(filteredAclOperations)){
            strayAcls = strayAcls.filterNot(x => (x.host() == "*" && x.permissionType() == AclPermissionType.ALLOW && consumerAclOperations.contains(x.operation())))
            var modifiedAcl = new AzPubSubAccessControlEntry(principal, "*", AclOperation.ANY, AclPermissionType.ALLOW, "CONSUMER")
            producerConsumerAclMap(resource._1).add(modifiedAcl)
          }
          strayAcls.foreach(acl => {
            var modifiedAcl = new AzPubSubAccessControlEntry(principal, acl.host(), acl.operation(), acl.permissionType(), "NONE")
            producerConsumerAclMap(resource._1).add(modifiedAcl)
          })
        }}
      }
      else if (resource._1.resourceType() == ResourceType.GROUP) {
        resource._2.foreach(acl => {
          //not filtering on allow or *
          var modifiedAcl = new AzPubSubAccessControlEntry(acl.principal(), acl.host(), acl.operation(), acl.permissionType(),"NONE")
          producerConsumerAclMap(resource._1).add(modifiedAcl)
        })
      }
    })
    return producerConsumerAclMap
  }
}

class AzPubSubAdminClientService(opts: AzPubSubAclCommandOptions) extends AdminClientService(opts) {

  override def listAcls(): Unit = {
    withAdminClient(opts) { adminClient =>
      val filters = getResourceFilter(opts, dieIfNoResourceFound = false)
      val listPrincipals = getPrincipals(opts, opts.listPrincipalsOpt)
      val resourceToAcls = getAcls(adminClient, filters)
      if (listPrincipals.isEmpty) {
        if(opts.options.has(opts.outputAsProducerConsumerOpt)){ //change it to producer consumer opt
          var producerConsumerAclMap = AzPubSubAclCommand.AclToProducerConsumerMapping(resourceToAcls);
          outputAsJson(producerConsumerAclMap)
          return;
        }
        if(opts.options.has(opts.outputAsJsonOpt)) {
          println("Received Acl information from Kafka")
          outputAsJson(resourceToAcls)
          return
        }
        for ((resource, acls) <- resourceToAcls) {
          println(s"Current ACLs for resource `$resource`: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
        }
      }
      else {
        val allPrincipalsFilteredResourceToAcls =  resourceToAcls.mapValues(acls =>
          acls.filterNot(acl => listPrincipals.forall(
            principal => !principal.toString.equals(acl.principal)))).filter(entry => entry._2.nonEmpty)

        if(opts.options.has(opts.outputAsProducerConsumerOpt)){ //change it to producer consumer opt
          var producerConsumerAclMap = AzPubSubAclCommand.AclToProducerConsumerMapping(allPrincipalsFilteredResourceToAcls)
          outputAsJson(producerConsumerAclMap)
          return;
        }

        if(opts.options.has(opts.outputAsJsonOpt)) {
          println("Received Acl information from Kafka")
          outputAsJson(allPrincipalsFilteredResourceToAcls)
          return
        }

        listPrincipals.foreach(principal => {
          println(s"ACLs for principal `$principal`")
          val filteredResourceToAcls =  resourceToAcls.mapValues(acls =>
            acls.filter(acl => principal.toString.equals(acl.principal))).filter(entry => entry._2.nonEmpty)
          for ((resource, acls) <- filteredResourceToAcls) {
            println(s"Current ACLs for resource `$resource`: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
          }
        })
      }
    }
  }

  def outputAsJson(filteredResourceToAcls: mutable.Map[ResourcePattern, mutable.Set[AzPubSubAccessControlEntry]]): Unit = {
    val resourceList = mutable.Set[Any]()
    for ((resource, acls) <- filteredResourceToAcls) {
      resourceList.add(mutable.Map("acls" -> acls.map(x => mutable.Map("principal" -> x.principal(),
        "host" -> x.host(),
        "operation" -> x.operation().toString,
        "aggregatedOperation" -> x.aggregatedOperation(),
        "permissionType" -> x.permissionType().toString).asJava).asJava,
        "resourceType" -> resource.resourceType().toString,
        "name" -> resource.name(), "patternType" -> resource.patternType().toString
      ).asJava)
    }
    val aclResponse = Json.encodeAsString(Map("resources" ->resourceList.asJava).asJava)
    println(aclResponse)
  }
}

class AzPubSubAclCommandOptions (args: Array[String]) extends AclCommandOptions(args.filterNot(x => (x == "--pc"))){
  //copy json opt here or get rid of it
  val outputAsProducerConsumerOpt = parser.accepts("pc", "output ACL list as producer consumer")
  options = parser.parse(args: _*)
}