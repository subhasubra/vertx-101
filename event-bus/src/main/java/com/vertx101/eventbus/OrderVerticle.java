package com.vertx101.eventbus;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;

/**
 * Generates Orders at regular intervals
 */
public class OrderVerticle extends AbstractVerticle {

  private final Random randomGen = new Random();
  private final Logger logger = LoggerFactory.getLogger(OrderVerticle.class);

  @Override
  public void start() {
    generateOrder();
  }

  /**
   * Generate orders at regular intervals (1s to 5s)
   */
  private void generateOrder() {
    vertx.setTimer(randomGen.nextInt(4000) + 1000, this::createOrder);
  }

  /**
   * Create an order and publish it onto event bus
   */
  private void createOrder(long timerId) {
    JsonObject order = new JsonObject()
      .put("ORDER_ID", UUID.randomUUID().toString())
      .put("ORDER_VALUE", randomGen.nextInt(5000));
    vertx.eventBus().publish("order.updates", order);
    logger.info("Create Order:", order.toString());
    generateOrder();
  }
}
