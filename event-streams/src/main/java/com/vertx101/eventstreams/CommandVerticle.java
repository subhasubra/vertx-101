package com.vertx101.eventstreams;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandVerticle extends AbstractVerticle {

  private final Logger logger = LoggerFactory.getLogger(CommandVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    vertx.createNetServer()
      .connectHandler(this::handleCommands)
      .listen(3000);
  }

  private void handleCommands(NetSocket netSocket) {
    RecordParser.newDelimited("\n", netSocket)
      .handler(buffer -> {
        String command = buffer.toString();
        switch(command) {
          case "/list": {
            vertx.eventBus().request("feed.list", "", response -> {
              if(response.succeeded()) {
                JsonObject data = (JsonObject) response.result().body();
                data.getJsonArray("stocks")
                  .stream().forEach(name -> netSocket.write(name + "\n"));
              } else {
                logger.error("/list error:", response.cause());
              }
            });
          }
            break;
          case "/begin":
            vertx.eventBus().send("feed.begin", "");
            break;
          case "/end":
            vertx.eventBus().send("feed.end", "");
            break;
          case "/halt":
            vertx.eventBus().send("feed.halt", "");
            break;
          case "/resume":
            vertx.eventBus().send("feed.resume", "");
            break;
          default:
            netSocket.write("Unknown command");
            break;
        }
      })
      .endHandler(v -> logger.info("Connection Ended"));
  }

}
