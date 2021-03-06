package com.vertx101.eventbus;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecondaryInstance {
  private static final Logger logger = LoggerFactory.getLogger(SecondaryInstance.class);
  public static void main(String [] args) {
    // Set Default Logger Factory to SLF4J
    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    Vertx.clusteredVertx(new VertxOptions(), handler -> {
      if(handler.succeeded()) {
        logger.info("Secondary Instance Started");
        Vertx vertx = handler.result();
        vertx.deployVerticle("com.vertx101.eventbus.OrderVerticle", new DeploymentOptions().setInstances(4));
        JsonObject conf = new JsonObject().put("port", 8081);
        vertx.deployVerticle("com.vertx101.eventbus.MainVerticle",
          new DeploymentOptions().setConfig(conf));
      } else {
        logger.error("Could not start the Main instance", handler.cause());
      }
    });
  }
}
