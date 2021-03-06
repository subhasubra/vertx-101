package com.vertx101.hellovertx;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.http.HttpClient;

@ExtendWith(VertxExtension.class)
public class TestHelloVerticle {

  @BeforeEach
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
    vertx.deployVerticle(new HelloVerticle(), testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  void verticle_deployed(Vertx vertx, VertxTestContext testContext) throws Throwable {
    HttpClient client = vertx.createHttpClient();
    client.request(HttpMethod.GET, 8088, "localhost", "/")
            .compose(req -> req.send().compose(HttpClientResponse::body))
            .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
              testContext.completeNow();
            })));

  }
}
