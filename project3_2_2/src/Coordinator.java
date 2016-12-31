import java.io.IOException;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.NamingEnumeration;

import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

import com.hazelcast.impl.base.KeyValue;


class HashFunction {
	// "a" -> 1; "b" -> 2; c-> 3
	public static Integer GetValue(String key) {
		if (key == null || key.length() == 0) return 0;
		return (key.charAt(0) - 'a') % 3 + 1;
	}
}

enum OperationEnum {
	PUT, GET
}

public class Coordinator extends Verticle {

    // This integer variable tells you what region you are in
    // 1 for US-E, 2 for US-W, 3 for Singapore
    private static int region = KeyValueLib.region;

    // Default mode: Strongly consistent
    // Options: strong, eventual
    private static String consistencyType = "strong";
    
    private final String THREAD_NAME_PREFIX = "Thread-";
    private AtomicInteger numberOfThreads = new AtomicInteger(0);

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances. Be sure to match the regions with their DNS!
     * Do the same for the 3 Coordinators as well.
     */
    private static final String dataCenterUSE = "ec2-54-172-202-46.compute-1.amazonaws.com";
    private static final String dataCenterUSW = "ec2-54-159-218-126.compute-1.amazonaws.com";
    private static final String dataCenterSING = "ec2-54-86-217-162.compute-1.amazonaws.com";

    private static final String coordinatorUSE = "ec2-52-201-235-73.compute-1.amazonaws.com";
    private static final String coordinatorUSW = "ec2-54-87-135-2.compute-1.amazonaws.com";
    private static final String coordinatorSING = "ec2-54-158-110-46.compute-1.amazonaws.com";
    
    private static final String[] sDataCenterDNS = new String[] {dataCenterUSE, dataCenterUSW, dataCenterSING};
    
    private ConcurrentHashMap<String, PriorityQueue<Long>> mWaitedTasksQueue = new ConcurrentHashMap<>();
    
    public void acquireLock(final String key, final Long timestamp) {
    	PriorityQueue<Long> thisKeyTaskQueue;
    	// Add timestamp
    	synchronized (mWaitedTasksQueue) {
			if (mWaitedTasksQueue.containsKey(key) == false) {
				//log("Create new queue for key: " + key);
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
				//log("Front timestamp: " + front + " with key: " + key);
				if (front.equals(timestamp)) {
					log(Thread.currentThread().getName() +" acquires lock; key = " + key + ", timestamp = " + timestamp);
					return;
				}
				else {
					//log("Fail: acquire lock; key = " + key + ", timestamp = " + timestamp);
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
    	final PriorityQueue<Long> thisKeyTaskQueue = mWaitedTasksQueue.get(key);
    	synchronized (thisKeyTaskQueue) {
    		log(Thread.currentThread().getName() + " releases lock. key = " + key + ", timestamp = " + timestamp);
			thisKeyTaskQueue.poll();
			thisKeyTaskQueue.notifyAll();
		}
    }
    
    private String getLocalDCDns(Integer regionIndex) {
    	//log("Get DC dns. region: " + regionIndex);
    	String targetDCDns = null;
    	for (String dns : KeyValueLib.dataCenters.keySet()) {
			if (KeyValueLib.dataCenters.get(dns).equals(regionIndex)) {
				targetDCDns = dns;
				break;
			}
		}
    	//log("DC dns: " + targetDCDns + " in region: " + regionIndex);
    	return targetDCDns;
    }
    
    private static void log(String content) {
    	System.out.println(content);
    }

    @Override
    public void start() {
        KeyValueLib.dataCenters.put(dataCenterUSE, 1);
        KeyValueLib.dataCenters.put(dataCenterUSW, 2);
        KeyValueLib.dataCenters.put(dataCenterSING, 3);
        KeyValueLib.coordinators.put(coordinatorUSE, 1);
        KeyValueLib.coordinators.put(coordinatorUSW, 2);
        KeyValueLib.coordinators.put(coordinatorSING, 3);
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
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                final String forwarded = map.get("forward");
                final String forwardedRegion = map.get("region");
                Thread t = new Thread(new Runnable() {
                	
                	private void crazilyPut(String[] dataCenterDNS, String key, String value, String timestamp) {
                		log(Thread.currentThread().getName() + " Crazily PUT Key: " + key + " Value: " + value + " Timestamp: " + timestamp);
                		//log(String.valueOf(dataCenterDNS.length));
                		for (String DNS : dataCenterDNS) {
                			//log("PUT in DC " + DNS);
							while (true) {
								try {
									KeyValueLib.PUT(DNS, key, value, timestamp, consistencyType);
									break;
								} catch (IOException e) {
									// Exception?? Retry until there's no exception!
									log("IO exception. Retry.");
									continue;
								}
							}
						}
                		//log("Success PUT Key: " + key + " Value: " + value);
                	}
                	
                	private void crazilyForward(String targetDns, String key, String value, String timestamp) {
                		log(Thread.currentThread().getName() + String.format(" Crazily FORWARD TargetDns: %s, Key: %s, Value: %s, Timestamp: %s", targetDns, key, value, timestamp));
                		while (true) {
                			try {
                				KeyValueLib.FORWARD(targetDns, key, value, timestamp);
                				break;
                			} catch (IOException e) {
                				log("IO exception. Retry.");
                				continue;
                			}
                		}
                		//log(String.format("Success: Crazily FORWARD TargetDns: %s, Key: %s, Value: %s, Timestamp: %s", targetDns, key, value, timestamp));
                	}
                	
                	private void crazilyAhead(String key, String timestamp) {
                		log(Thread.currentThread().getName() + String.format("Crazily AHEAD Key: %s, Timestamp: %s", key, timestamp));
                		while (true) {
                			try {
								KeyValueLib.AHEAD(key, timestamp);
								break;
							} catch (IOException e) {
								log("IO exception. Retry.");
								continue;
							}
                		}
                		//log(String.format("Success: Crazily AHEAD Key: %s, Timestamp: %s", key, timestamp));
                	}
                	
                	private void crazilyComplete(String key, String timestamp) {
                		log(Thread.currentThread().getName() + String.format("Crazily COMPLETE Key: %s, Timestamp: %s", key, timestamp));
                		while (true) {
                			try {
								KeyValueLib.COMPLETE(key, timestamp);
								break;
							} catch (IOException e) {
								log("IO exception. Retry.");
								continue;
							}
                		}
                		//log(String.format("Success: Crazily COMPLETE Key: %s, Timestamp: %s", key, timestamp));
                	}
                	
                    public void run() {
                    /* Each operation is handled in a new thread.
                     * Use of helper functions is highly recommended */
                    	
                    	// if the request is not forwarded, AHEAD all DCs
                    	// TODO: if eventual, no need to ahead
                    	if (consistencyType.equals("strong")) {
                    		if (forwarded == null) {
                        		crazilyAhead(key, timestamp.toString()); // if req is forwarded, there's no need to Ahead coz this's been done before forwarding
                        	}
                        	else if (!forwarded.equals("true")) {
                        		crazilyAhead(key, timestamp.toString()); // if req is forwarded, there's no need to Ahead coz this's been done before forwarding
                        	}
                    	}

                    	String threadName = Thread.currentThread().getName();
                    	log(String.format("%s processes PUT. key: %s, timestamp: %d, value: %s, forwarded: %s", threadName, key, timestamp, value, forwarded));
                    	if (consistencyType.equals("strong"))
                    		acquireLock(key, timestamp);
                    	Integer primaryRegionIndex = HashFunction.GetValue(key);
                    	if (!primaryRegionIndex.equals(region)) {
                    		// forward to the primary region
                    		// get primary region DNS
                    		String forwardingTargetCoordinatorDns = "";
                    		for (String dns : KeyValueLib.coordinators.keySet()) {
								if (primaryRegionIndex.equals(KeyValueLib.coordinators.get(dns))) {
									forwardingTargetCoordinatorDns = dns;
									break;
								}
							}
                    		crazilyForward(forwardingTargetCoordinatorDns, key, value, timestamp.toString());
                    		if (consistencyType.equals("strong"))
                    			releaseLock(key, timestamp);
                    		return;
                    	}
                    	
                    	crazilyPut(sDataCenterDNS, key, value, timestamp.toString());
                    	if (consistencyType.equals("strong")) {
                    		crazilyComplete(key, timestamp.toString());
                    		releaseLock(key, timestamp);
                    	}
                    		
                    }
                }, THREAD_NAME_PREFIX + numberOfThreads.getAndIncrement());
                t.start();
                req.response().end(); // Do not remove this
            }
        });
        
        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                Thread t = new Thread(new Runnable() {
                	
                	private String crazilyGet(final String targetDns, final String key, final String timestamp) {
                		log(Thread.currentThread().getName() + String.format(" GET dcDns: %s, Key: %s, Timestamp: %s", targetDns, key, timestamp));
                		String value = "";
                		while (true) {
                			try {
                				value = KeyValueLib.GET(targetDns, key, timestamp, consistencyType);
                				break;
                			} catch (IOException e) {
                				// Exception?? Retry until there's no exception!
                				continue;
                			}
                		}
                		//log(String.format("finish GET dcDns: %s, Key: %s, Timestamp: %s", targetDns, key, timestamp));
                		if (value.equals("null")) value = "0";
                		return value;
                	}
                	
                    public void run() {
                    /* TODO: Add code for GET requests handling here
                     * Each operation is handled in a new thread.
                     * Use of helper functions is highly recommended */
                    	String threadName = Thread.currentThread().getName();
                    	log(String.format("%s processes GET. key: %s, timestamp: %d", threadName, key, timestamp));
                    	if (consistencyType.equals("strong"))
                    		acquireLock(key, timestamp);
                    	//log(String.format("GET key: %s, timestamp: %d", key, timestamp));
                        String response = "0";
                        String localDCDns = getLocalDCDns(region);
                        if (consistencyType.equals("strong"))
                        	releaseLock(key, timestamp);
                        response = crazilyGet(localDCDns, key, timestamp.toString());
                        //log(String.format("GET key: %s, timestamp: %d, value: %s", key, timestamp, response));

                        req.response().end(response);
                        
                    }
                }, THREAD_NAME_PREFIX + numberOfThreads.getAndIncrement());
                t.start();
            }
        });
        /* This endpoint is used by the grader to change the consistency level */
        routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                consistencyType = map.get("consistency"); // 'strong' or 'eventual'
                log(Thread.currentThread().getName() + " get CONSISTENCY type: " + consistencyType);
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

