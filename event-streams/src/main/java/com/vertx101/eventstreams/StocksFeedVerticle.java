package com.vertx101.eventstreams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class StocksFeedVerticle extends AbstractVerticle {

  // load file name from config/env
  private String STOCKS_FILE = null;
  private JsonArray stocks = null;
  private final Logger logger = LoggerFactory.getLogger(StocksFeedVerticle.class);
  private final Set<HttpServerResponse> streamers = new HashSet<>();
  private enum STATE  {LIVE, HALTED};
  private STATE currentState = STATE.HALTED;
  private Random randGen = new Random();
  private final String snapshotDir = "/tmp/snapshots";
  private final ObjectMapper objMapper = new ObjectMapper();

  /**
   * Handler for HTTP Requests
   * @param request
   */
  private void httpHandler(HttpServerRequest request) {
    logger.info("Http request {} received", request.path());
    if (request.path().equals("/")) {
      openStocksFeed(request);
      return;
    } else if (request.path().equals("/download/")) {
      download(request);
      return;
    } else {
      request.response().setStatusCode(404).end();
    }
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    // Read Config
    ConfigRetriever configRetriever = ConfigRetriever.create(vertx);

    configRetriever.getConfig(ar -> {
        if (ar.failed()) {
          // do something
          logger.error("Config load failed", ar.cause());
        } else {
          JsonObject config = ar.result();
          STOCKS_FILE = config.getString("stocks");
          logger.info("stock file path: {}", STOCKS_FILE);
        }
      });

    // Setup Event Handlers
    EventBus eventBus = vertx.eventBus();
    eventBus.consumer("feed.list", this::list);
    eventBus.consumer("feed.begin", this::begin);
    eventBus.consumer("feed.end", this::end);
    eventBus.consumer("feed.halt", this::halt);
    eventBus.consumer("feed.resume", this::resume);

    // Initialise the Stocks
    setStocks();


    // Initialise HTTP Server
    vertx.createHttpServer().requestHandler(this::httpHandler)
      .listen(8888, http -> {
        if (http.succeeded()) {
          startPromise.complete();
          System.out.println("HTTP server started on port 8888");
        } else {
          startPromise.fail(http.cause());
        }
      });

    // Periodically generate the stocks feed
    vertx.setPeriodic(10000, this::generateStocksFeed);
  }

  private boolean setStocks() {
    logger.info("Initialising stocks");

    // Check if the file exists, else exit
    if(!vertx.fileSystem().existsBlocking(this.STOCKS_FILE)) {
      logger.error("Unable to open file for stocks read: {}", STOCKS_FILE);
      return false;
    }

    vertx.fileSystem().readFile(this.STOCKS_FILE, handler -> {
      if (handler.succeeded()) {
        List<String> list = handler.result()
          .toJsonArray()
          .stream()
          .map(JsonObject.class::cast)
          .map(name -> name.getValue("name").toString())
          .collect(Collectors.toList());
        stocks = new JsonArray(list);
        logger.info("Stocks:{}", stocks.toString());
      } else {
        logger.error("File Read failed...", handler.cause());
        //request.fail(500, handler.cause().getMessage());
      }
    });
    if(stocks == null)
      return false;
    else
      return true;
  }

  /**
   * Generate Stocks Feed
   * @param l
   */
  private void generateStocksFeed(long l) {
    if (currentState == STATE.HALTED)
      return;
    if (stocks == null) {
      logger.error("Stocks not initialised");
      return;
    }
    List<StockFeed> stockFeeds = stocks.stream()
      .map(v -> {
        String stock = (String) v;
        StockFeed sf = new StockFeed();
        sf.setValues(stock, randGen.nextInt(5000) + 1.0);
        return sf;
      })
      .collect(Collectors.toList());

    // Save it to snapshot that can be downloaded later (for now one file per day)
    try {
      Buffer data = Buffer.buffer(objMapper.writeValueAsString(stockFeeds));
      snapshot(data);

      for (HttpServerResponse streamer : streamers) {
        if (!streamer.writeQueueFull())
          streamer.write(data);
      }
    } catch (JsonProcessingException ex) {
      logger.error("Error while creating buffer: {}", ex.getMessage());
    }
  }

  private void snapshot(Buffer data) {
    // Create file (day-wise)
    String fileName = this.snapshotDir + "/" + "SNAPSHOT-" + DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now());

    // Check if the file exists, else create followed by write
    if(!vertx.fileSystem().existsBlocking(fileName)) {
      logger.info("File does not exist. Creating {}", fileName);
      vertx.fileSystem().createFile(fileName).onFailure(hdlr -> {
        logger.error("Unable to create file for snapshot write:", hdlr.getCause());
        return;
      });
    }

    vertx.fileSystem().open(fileName, new OpenOptions().setAppend(true), hdlr -> {
      if (hdlr.succeeded()) {
        AsyncFile file = hdlr.result();
        file.write(data);
        if (file.writeQueueFull()) {
          file.pause();
          file.drainHandler(v -> file.resume());
        }
        hdlr.result().close();
      } else {
        logger.error("Unable to open file for snapshot write");
      }
    });
  }

  /**
   * Open a stream for stocks data feed
   * @param request
   */
  private void openStocksFeed(HttpServerRequest request) {
    HttpServerResponse response = request.response()
      .putHeader("Content-Type", "application/json")
      .setChunked(true);
    streamers.add(response);
    logger.info("A streamer joined");
    response.endHandler(handler -> {
      streamers.remove(response);
      logger.info("A streamer left");
    });
  }

  /**
   * Download Snapshot data
   * @param request
   */
  private void download(HttpServerRequest request) {
    String fileName = this.snapshotDir + "/" + "SNAPSHOT-" + DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now());
    // If file does not exist, throw error and exit
    if (!vertx.fileSystem().existsBlocking(fileName)) {
      request.response().setStatusCode(404).end();
      return;
    }
    vertx.fileSystem().open(fileName, new OpenOptions().setRead(true), handler -> {
      if(handler.succeeded()) {
        HttpServerResponse response = request.response();
        response.setStatusCode(200)
          .putHeader("Content-Type", "application/json")
          .setChunked(true);
        AsyncFile file = handler.result();
        file.handler(buffer -> {
          response.write(buffer);
          if (response.writeQueueFull()) {
            file.pause();
            response.drainHandler(v -> file.resume());
          }
        });
        file.endHandler(v -> response.end());
      } else {
        logger.error("File read failed");
        request.response().setStatusCode(500).end();
      }
    });
  }

  /**
   * Event handler for listing all the stocks whose feeds are streamed
   * @param request
   */
  private void list(Message request) {
    logger.info("list handler invoked");
    // We assume the stocks list is available in a file
    if (stocks == null) {
        logger.error("Stocks File not initialised");
        request.fail(500, "Stocks File not initialised");
    } else {
      JsonObject json = new JsonObject().put("stocks", stocks);
      logger.info("Stocks List:", json.toString());
      request.reply(json);
    }
  }

  private void begin(Message request) {
    logger.info("begin handler invoked");
    System.out.println("begin handler invoked");
    this.currentState = STATE.LIVE;
  }

  private void end(Message request) {
    logger.info("enc handler invoked");
    this.currentState = STATE.HALTED;
  }

  private void halt(Message request) {
    logger.info("halt handler invoked");
    this.currentState = STATE.HALTED;
  }

  private void resume(Message request) {
    logger.info("resume handler invoked");
    this.currentState = STATE.LIVE;
  }

  public static void main(String [] args) {
    // Set Default Logger Factory to SLF4J
    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory.class.getName());
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle("com.vertx101.eventstreams.CommandVerticle");
    vertx.deployVerticle("com.vertx101.eventstreams.StocksFeedVerticle");
  }
}
