package com.vertx101.eventbus;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Order Consumer
 * Does nothing but logs the orders
 */
public class OrderProcessorVerticle extends AbstractVerticle {

  private final Logger logger = LoggerFactory.getLogger(OrderProcessorVerticle.class);

  @Override
  public void start() {
    EventBus eventBus = vertx.eventBus();
    eventBus.<JsonObject>consumer("order.updates", message -> {
      JsonObject order = (JsonObject) message.body();
      //System.out.println("OrderProcessor Consumer invoked...");
      logger.info("OrderProcessor Consumer invoked...");
      logger.info("New Order {} received with Order Value {}", order.getString("ORDER_ID"), order.getInteger("ORDER_VALUE"));
    });
  }
}
