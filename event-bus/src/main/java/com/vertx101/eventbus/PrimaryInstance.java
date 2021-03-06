package com.vertx101.eventbus;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimaryInstance {
  private static final Logger logger = LoggerFactory.getLogger(PrimaryInstance.class);
  /**
   * Clustered (Multiple Instances) Deployment Example
   * @param args
   */
  public static void main(String [] args) {
    // Set Default Logger Factory to SLF4J
    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    //ClusterManager cm = new InfinispanClusterManager();
    Vertx.clusteredVertx(new VertxOptions(), handler -> {
      if(handler.succeeded()) {
        logger.info("Primary Instance Started");
        Vertx vertx = handler.result();
        vertx.deployVerticle("com.vertx101.eventbus.OrderVerticle", new DeploymentOptions().setInstances(4));
        vertx.deployVerticle("com.vertx101.eventbus.OrderProcessorVerticle");
        vertx.deployVerticle("com.vertx101.eventbus.MainVerticle");
      } else {
        logger.error("Could not start the Primary instance", handler.cause());
      }
    });
  }
}
