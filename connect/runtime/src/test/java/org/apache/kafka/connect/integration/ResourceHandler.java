package org.apache.kafka.connect.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceHandler {

  private Thread resource;
  private static final Logger log =
      LoggerFactory.getLogger(ResourceHandler.class);

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

  public boolean isResourceAlive() {
    return resource != null && resource.isAlive();
  }

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
