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

package kafka.server.metadata

import kafka.utils.Logging
import org.apache.kafka.image.loader.{LoaderManifest, LoaderManifestType}
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.fault.FaultHandler

import scala.concurrent.TimeoutException


class AclPublisher(
  nodeId: Int,
  faultHandler: FaultHandler,
  nodeType: String,
  authorizer: Option[Authorizer],
) extends Logging with org.apache.kafka.image.publisher.MetadataPublisher {
  logIdent = s"[${name()}] "

  override def name(): String = s"AclPublisher $nodeType id=$nodeId"

  private var completedInitialLoad = false

  override def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
    manifest: LoaderManifest
  ): Unit = {
    val deltaName = s"MetadataDelta up to ${newImage.offset()}"

    // Apply changes to ACLs. This needs to be handled carefully because while we are
    // applying these changes, the Authorizer is continuing to return authorization
    // results in other threads. We never want to expose an invalid state. For example,
    // if the user created a DENY ALL acl and then created an ALLOW ACL for topic foo,
    // we want to apply those changes in that order, not the reverse order! Otherwise
    // there could be a window during which incorrect authorization results are returned.
    Option(delta.aclsDelta()).foreach { aclsDelta =>
      authorizer match {
        case Some(authorizer: ClusterMetadataAuthorizer) => if (manifest.`type`().equals(LoaderManifestType.SNAPSHOT)) {
          try {
            // If the delta resulted from a snapshot load, we want to apply the new changes
            // all at once using ClusterMetadataAuthorizer#loadSnapshot. If this is the
            // first snapshot load, it will also complete the futures returned by
            // Authorizer#start (which we wait for before processing RPCs).
            info(s"Loading authorizer snapshot at offset ${newImage.offset()}")
            authorizer.loadSnapshot(newImage.acls().acls())
          } catch {
            case t: Throwable => faultHandler.handleFault("Error loading " +
              s"authorizer snapshot in $deltaName", t)
          }
        } else {
          try {
            // Because the changes map is a LinkedHashMap, the deltas will be returned in
            // the order they were performed.
            aclsDelta.changes().forEach((key, value) =>
              if (value.isPresent) {
                authorizer.addAcl(key, value.get())
              } else {
                authorizer.removeAcl(key)
              })
          } catch {
            case t: Throwable => faultHandler.handleFault("Error loading " +
              s"authorizer changes in $deltaName", t)
          }
        }
        if (!completedInitialLoad) {
          // If we are receiving this onMetadataUpdate call, that means the MetadataLoader has
          // loaded up to the local high water mark. So we complete the initial load, enabling
          // the authorizer.
          completedInitialLoad = true
          authorizer.completeInitialLoad()
        }
        case _ => // No ClusterMetadataAuthorizer is configured. There is nothing to do.
      }
    }
  }

  override def close(): Unit = {
    authorizer match {
      case Some(authorizer: ClusterMetadataAuthorizer) => authorizer.completeInitialLoad(new TimeoutException)
      case _ =>
    }
  }
}
