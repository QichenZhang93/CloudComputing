import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
    /* TODO: Add code to implement your backend storage */

    @Override
    public void start() {
        final KeyValueStore keyValueStore = new KeyValueStore();
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);
        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                String key = map.get("key");
                String value = map.get("value");
                String consistency = map.get("consistency");
                Integer region = Integer.parseInt(map.get("region"));

                Long timestamp = Long.parseLong(map.get("timestamp"));

                /* TODO: Add code here to handle the put request
                     Remember to use the explicit timestamp if needed! */
                String response = "stored";
                req.response().putHeader("Content-Type", "text/plain");
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });
        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                String consistency = map.get("consistency");
                final Long timestamp = Long.parseLong(map.get("timestamp"));

                /* TODO: Add code here to handle the get request
                     Remember that you may need to do some locking for this */
                String response = "";
                req.response().putHeader("Content-Type", "text/plain");
                if (response != null)
                    req.response().putHeader("Content-Length",
                            String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });
        // Clears this stored keys. Do not change this
        routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                /* TODO: Add code to here to flush your datastore. This is MANDATORY */
                req.response().putHeader("Content-Type", "text/plain");
                req.response().end();
                req.response().close();
            }
        });
        // Handler for when the AHEAD is called
        routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                String key = map.get("key");
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                /* TODO: Add code to handle the signal here if you wish */
                req.response().putHeader("Content-Type", "text/plain");
                req.response().end();
                req.response().close();
            }
        });
        // Handler for when the COMPLETE is called
        routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                String key = map.get("key");
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                /* TODO: Add code to handle the signal here if you wish */
                req.response().putHeader("Content-Type", "text/plain");
                req.response().end();
                req.response().close();
            }
        });
        routeMatcher.noMatch(new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                req.response().putHeader("Content-Type", "text/html");
                String response = "Not found.";
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });
        server.requestHandler(routeMatcher);
        server.listen(8080);
    }
}

