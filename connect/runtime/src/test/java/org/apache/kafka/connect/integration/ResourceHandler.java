package org.apache.kafka.connect.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A handler class for connector's resource management.
 */
public class ResourceHandler {

  private Thread resource;
  private static final Logger log =
      LoggerFactory.getLogger(ResourceHandler.class);

  /**
   * Create a resource that will be closed when the connector is stopped.
   */
  public void createResource() {
    resource = new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          break;
        }
      }
    });
    resource.start();
  }

  /**
   * Check if the resource is alive.
   * @return true if the resource is alive, false otherwise.
   */
  public boolean isResourceAlive() {
    return resource != null && resource.isAlive();
  }

  /**
   * Close the resource.
   */
  public void closeResource() {
    if (resource != null) {
      resource.interrupt();
      try {
        resource.join();
      } catch (InterruptedException e) {
        log.debug("Interrupted while waiting for thread to join", e);
      }
    }
  }
}
