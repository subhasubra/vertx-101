package com.vertx101.eventbus;

import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Class that starts the Web Server and other verticles
 */
public class MainVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    vertx.createHttpServer().requestHandler(this::handler)
      .listen(8888, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        logger.info("HTTP server started on port 8888");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }

  private void handler(HttpServerRequest request) {
    if(request.path().equals("/")) {
      request.response()
        .putHeader("content-type", "text/plain")
        .end("This is a Vert.x Event Bus Sample Program! Use /sse to view updated data from server!");
    } else if(request.path().equals("/sse")) {
      sseHandler(request);
    } else {
      request.response().setStatusCode(404);
    }
  }

  /**
   * Handler for Server Sent Events
   * @param request
   */
  private void sseHandler(HttpServerRequest request) {
    HttpServerResponse response = request.response();
    response.putHeader("content-type", "text/event-stream") // MIME type for sse
      .putHeader("cache-control", "no-cache") // ensure no browser caching
      .setChunked(true);

    MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer("order.updates");
    consumer.handler(message -> {
      //System.out.println(message.toString());
      response.write("event: order update\n"); // first line is event
      response.write("data: " + message.body().encode() + "\n\n"); // next line is event data followed by a new line
    });

    response.endHandler(h -> consumer.unregister());

  }

  /**
   * Single Instance Example
   * @param args
   */
  public static void main(String [] args) {
    // Set Default Logger Factory to SLF4J
    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle("com.vertx101.eventbus.OrderVerticle", new DeploymentOptions().setInstances(4));
    vertx.deployVerticle("com.vertx101.eventbus.OrderProcessorVerticle");
    vertx.deployVerticle("com.vertx101.eventbus.MainVerticle");
  }
}
