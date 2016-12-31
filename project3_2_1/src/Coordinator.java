import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

enum OperationEnum {
	PUT, GET
}

public class Coordinator extends Verticle {
	
    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances
     */
    private static final String dataCenter1 = "ec2-54-162-72-214.compute-1.amazonaws.com";
    private static final String dataCenter2 = "ec2-54-162-27-100.compute-1.amazonaws.com";
    private static final String dataCenter3 = "ec2-54-86-190-166.compute-1.amazonaws.com";
    
    private static final String[] sDataCenterDNS = new String[] {dataCenter1, dataCenter2, dataCenter3};
    
    private ConcurrentHashMap<String, HashMap<OperationEnum, Integer>> mOngoingTasks = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, PriorityQueue<Long>> mWaitedTasksQueue = new ConcurrentHashMap<>();
    
    private static void log(String content) {
    	System.out.println(content);
    }
    
    public void acquireLock(final String key, final Long timestamp) {
    	PriorityQueue<Long> thisKeyTaskQueue;
    	// Add timestamp
    	synchronized (mWaitedTasksQueue) {
			if (mWaitedTasksQueue.containsKey(key) == false) {
				log("Create new queue for key: " + key);
				// TODO: Problem here: always build a new PQ for this key
				mWaitedTasksQueue.put(key, new PriorityQueue<Long>());
			}
			thisKeyTaskQueue = mWaitedTasksQueue.get(key);
			thisKeyTaskQueue.add(timestamp);
		}
    	// Acquire lock
    	synchronized (thisKeyTaskQueue) {
			Long front;
			while (true) {
				front = thisKeyTaskQueue.peek();
				log("Front timestamp: " + front + " with key: " + key);
				if (front.equals(timestamp)) {
					log("acquire lock; key = " + key + ", timestamp = " + timestamp);
					return;
				}
				else {
					log("Fail: acquire lock; key = " + key + ", timestamp = " + timestamp);
					try {
						thisKeyTaskQueue.wait();
					} catch (InterruptedException e) {
						log("Unexpected exception!");
						e.printStackTrace();
					}
				}
			}
		}
    }
    
    public void releaseLock(final String key, final Long timestamp) {
    	log("release lock; key = " + key + ", timestamp = " + timestamp);
    	final PriorityQueue<Long> thisKeyTaskQueue = mWaitedTasksQueue.get(key);
    	synchronized (thisKeyTaskQueue) {
			thisKeyTaskQueue.poll();
			thisKeyTaskQueue.notifyAll();
		}
    }
    
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
                //final String timestamp = new Timestamp(System.currentTimeMillis() 
                                                //+ TimeZone.getTimeZone("EST").getRawOffset()).toString();
                final Long timestamp = System.currentTimeMillis(); // For ordering
                Thread t = new Thread(new Runnable() {
                	
                	/**
                	 * Put key-value into data centers.
                	 * @param dataCenterDns: Collections of DNS of data center
                	 */
                	private void crazilyPut(String[] dataCenterDNS, String key, String value) {
                		log("PUT Key: " + key + " Value: " + value);
                		for (String DNS : dataCenterDNS) {
                			log("PUT in DC " + DNS);
							while (true) {
								try {
									KeyValueLib.PUT(DNS, key, value);
									break;
								} catch (IOException e) {
									// Exception?? Retry until there's no exception!
									log("IO exception. Retry.");
									continue;
								}
							}
						}
                		log("Successfully PUT Key: " + key + " Value: " + value);
                	}
                	
                    public void run() {
                        //Each PUT operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.
                    	acquireLock(key, timestamp);
                    	final HashMap<OperationEnum, Integer> thisKeyOnGoingTasksHashMap;
                    	synchronized (mOngoingTasks) {
							if (!mOngoingTasks.containsKey(key)) {
								mOngoingTasks.put(key, new HashMap<OperationEnum, Integer>());
							}
							thisKeyOnGoingTasksHashMap = mOngoingTasks.get(key);
						}
                    	synchronized (thisKeyOnGoingTasksHashMap) {
							if (thisKeyOnGoingTasksHashMap.isEmpty()) { // GO GO GO!
								log("Start PUT; Key = " + key + ", Value = " + value +  ", Timestamp = " + timestamp);
								thisKeyOnGoingTasksHashMap.put(OperationEnum.PUT, 1); // There can be only one PUT
								crazilyPut(sDataCenterDNS, key, value);
								// finish
								thisKeyOnGoingTasksHashMap.remove(OperationEnum.PUT);
								log("Finish PUT; Key = " + key + ", Value = " + value +  ", Timestamp = " + timestamp);
								// Wake up potential GET. But no get will acquire lock until PUT finishes.
								// thisKeyOnGoingTasksHashMap.notifyAll();
							}
							else { // Wait on this HashSet until another operation is done.
								log("Wait for another task"); // a GET because GET will release lock before it's done to enable concurrent GET
								try {
									thisKeyOnGoingTasksHashMap.wait();
								} catch (InterruptedException e) {
									log("Unexpected exception occurs");
									e.printStackTrace();
								}
							}
						}
                    	releaseLock(key, timestamp);
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
                //final String timestamp = new Timestamp(System.currentTimeMillis() 
                	//+ TimeZone.getTimeZone("EST").getRawOffset()).toString();
                final Long timestamp = System.currentTimeMillis(); // For ordering
                Thread t = new Thread(new Runnable() {
                	
                	private String crazilyGet(final String key, final String loc) {
                		log("GET Key: " + key + " Loc: " + loc);
                		final Integer IntegerLoc;
                		try {
                			IntegerLoc = Integer.valueOf(loc);
                		}
                		catch (NumberFormatException e) {
							log("Loc parameter is not a valid integer");
							return null;
						}
                		// find datacenter dns according to loc
                		String DCDns = null;
                		for (String dns : KeyValueLib.dataCenters.keySet()) {
							if (KeyValueLib.dataCenters.get(dns).equals(IntegerLoc)) {
								DCDns = dns;
							}
						}
                		if (DCDns.equals("")) {
                			log("Cannot find DC with loc = "+ IntegerLoc);
                			return null;
                		}
                		String value = "0";
                		while (true) {
                			try {
                				value = KeyValueLib.GET(DCDns, key);
                				break;
                			} catch (IOException e) {
                				// Exception?? Retry until there's no exception!
                				log("IO exception. Retry.");
                				continue;
                			}
                		}
                		log("Successfully GET Key: " + key + " Loc: " + loc);
                		if (value.equals("null")) value = "0";
                		return value;
                	}
                	
                    public void run() {
                        //TODO: Write code for GET operation here.
                        //Each GET operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.
                    	acquireLock(key, timestamp);
                    	String value = null;
                    	final HashMap<OperationEnum, Integer> thisKeyOnGoingTasksHashMap;

                    	// Create hashMap if non exists
                    	synchronized (mOngoingTasks) {
							if (!mOngoingTasks.containsKey(key)) {
								mOngoingTasks.put(key, new HashMap<OperationEnum, Integer>());
							}
							thisKeyOnGoingTasksHashMap = mOngoingTasks.get(key);
						}
                    	
                    	// Enter onGoingTasks
                    	synchronized (thisKeyOnGoingTasksHashMap) {
							if (thisKeyOnGoingTasksHashMap.isEmpty()) { // GO GO GO!
								log("Start PUT; Key = " + key + ", Loc = " + loc +  ", Timestamp = " + timestamp);
								thisKeyOnGoingTasksHashMap.put(OperationEnum.GET, 1);
							}
							else { // Wait on this HashSet until another operation is done.
								if (thisKeyOnGoingTasksHashMap.containsKey(OperationEnum.PUT)) {
									log("Wait for another PUT. This shouldn't happen. If it does, check your code!");
									System.exit(-1);
								}
								Integer preNumber = thisKeyOnGoingTasksHashMap.get(OperationEnum.GET);
								log("Increment ongoing GET number from " + preNumber);
								thisKeyOnGoingTasksHashMap.put(OperationEnum.GET, preNumber + 1);
							}
							releaseLock(key, timestamp); // release lock early to enable concurrent GET.
						}
                    	value = crazilyGet(key, loc);
                    	synchronized (thisKeyOnGoingTasksHashMap) {
                    		Integer preNumber = thisKeyOnGoingTasksHashMap.get(OperationEnum.GET);
							log("Increment ongoing GET number from " + preNumber);
							if (preNumber.equals(1)) {
								thisKeyOnGoingTasksHashMap.remove(OperationEnum.GET);
							}
							else thisKeyOnGoingTasksHashMap.put(OperationEnum.GET, preNumber - 1);
							thisKeyOnGoingTasksHashMap.notifyAll(); // To wake up potential waiting PUT.
						}
                    	req.response().end(value); 
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

