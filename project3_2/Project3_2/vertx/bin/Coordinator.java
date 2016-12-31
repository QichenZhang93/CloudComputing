import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

import java.net.URL;
import java.sql.Timestamp;
import java.util.TimeZone;

public class Coordinator extends Verticle {

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances
     */
    private static final String dataCenter1 = "<DNS-OF-DATACENTER-1>";
    private static final String dataCenter2 = "<DNS-OF-DATACENTER-2>";
    private static final String dataCenter3 = "<DNS-OF-DATACENTER-3>";

    @Override
    public void start() {
        //DO NOT MODIFY THIS
        KeyValueLib.dataCenters.put(dataCenter1, 1);
        KeyValueLib.dataCenters.put(dataCenter2, 2);
        KeyValueLib.dataCenters.put(dataCenter3, 3);
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);

        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String value = map.get("value");
                //You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis() 
                                                + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        //TODO: Write code for PUT operation here.
                        //Each PUT operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.
                    }
                });
                t.start();
                
                // Every important notice should be repeated for three times
                //Do not remove this
                //Do not remove this
                //Do not remove this
                req.response().end(); 

            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String loc = map.get("loc");
                //You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis() 
                                + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        //TODO: Write code for GET operation here.
                        //Each GET operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.
                        req.response().end("0"); 
                    }
                });
                t.start();
            }
        });


        routeMatcher.get("/flush", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                //Flush all datacenters before each test.
                URL url = null;
                try {
                    url = new URL("http://" + dataCenter1 + ":8080/flush");
                    url.openConnection();
                    url = new URL("http://" + dataCenter2 + ":8080/flush");
                    url.openConnection();
                    url = new URL("http://" + dataCenter3 + ":8080/flush");
                    url.openConnection();
                } catch (Exception e) {

                }
                //This endpoint will be used by the auto-grader to flush your datacenter before tests
                //You can initialize/re-initialize the required data structures here
                req.response().end();
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

