package com.vertx101.hellovertx;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloVerticle extends AbstractVerticle {

  private final Logger logger = LoggerFactory.getLogger(HelloVerticle.class);
  private static int numConnections = 0;

  @Override
  public void start(Promise<Void> startPromise) /* throws Exception */ {

    // Configure Http Server by reading port from config (Env)
    ConfigRetriever configRetriever = ConfigRetriever.create(vertx);

    configRetriever.getConfig(ar -> {
      if (ar.failed()) {
        // do something
        System.out.println("Config load failed");
      } else {
        JsonObject config = ar.result();
        vertx.createHttpServer().requestHandler(req -> {
          req.response()
                  .putHeader("content-type", "text/plain")
                  .end(numClients());
        }).listen(config.getInteger("http.port"), http -> {
          if (http.succeeded()) {
            startPromise.complete();
            logger.info("HTTP server started on port " + config.getInteger("http.port"));
            //System.out.println("HTTP server started on port " + config.getInteger("http.port"));
          } else {
            startPromise.fail(http.cause());
          }
        });
      }
    });

    // Consume Hello from Clients
    vertx.createNetServer()
            .connectHandler(HelloVerticle::handleNewClient)
            .listen(3000);

    vertx.setPeriodic(5000, handler -> logger.info(numClients()));

  }

  private static void handleNewClient(NetSocket socket) {
    numConnections++;
    socket.handler(buffer -> {
      socket.write("Hello!");
      if (buffer.toString().endsWith("/quit\n")) {
        socket.close();
      } else {
        System.out.println("Client says:" + buffer.toString());
      }
    });
    socket.closeHandler(v -> numConnections--);
  }

  private static String numClients() {
    return "Hello from " + numConnections + " Clients";
  }

  public static void main(String [] args) {
    // Set Default Logger Factory to SLF4J
    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new HelloVerticle());
  }
}
