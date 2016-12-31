import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
    /* TODO: Add code to implement your backend storage */
    private void log(String content) {
    	System.out.println(content);
    }
    
	final ConcurrentHashMap<String, PriorityBlockingQueue<Long>> keyTimestampPriorityQueue = new ConcurrentHashMap<String, PriorityBlockingQueue<Long>>();
	final ConcurrentHashMap<String, String> miniDataStorage = new ConcurrentHashMap<String, String>();
	final ConcurrentHashMap<String, Integer> aheadCompleteLock = new ConcurrentHashMap<>();
	final ConcurrentHashMap<String, AtomicInteger> workingGetNumber = new ConcurrentHashMap<>(); // Get do not block. So need this when a new put comes.
	
	private final String THREAD_NAME_PREFIX = "Thread-";
    private AtomicInteger numberOfThreads = new AtomicInteger(0);
    
    private ConcurrentHashMap<String, Long> keyLatestTimeStamp = new ConcurrentHashMap<>();
	
	private void acquireLock(String key, Long timestamp) {
		PriorityBlockingQueue<Long> thisKeyPriorityQueue;
		synchronized (keyTimestampPriorityQueue) {
			if (!keyTimestampPriorityQueue.containsKey(key)) {
				keyTimestampPriorityQueue.put(key, new PriorityBlockingQueue<>());
			}
			thisKeyPriorityQueue = keyTimestampPriorityQueue.get(key);
		}
		synchronized (thisKeyPriorityQueue) {
			thisKeyPriorityQueue.add(timestamp);
			Long front;
			while (true) {
				front = thisKeyPriorityQueue.peek();
				if (front.equals(timestamp)) {
					log(Thread.currentThread().getName() + String.format(" acquires lock. key: %s, timestamp: %d", key, timestamp));
					return;
				}
				else {
					try {
						////log(String.format("fail: acquire lock. key: %s, timestamp: %d", key, timestamp));
						thisKeyPriorityQueue.wait();
					} catch (InterruptedException e) {
						//log("Wait() is interrupted! Check your code!");
						continue;
					}
				}
			}
		}
	}
	
	private void releaseLock(String key, Long timestamp) {
		log(Thread.currentThread().getName() + String.format(" releases lock. key: %s, timestamp: %d", key, timestamp));
		PriorityBlockingQueue<Long> thisKeyPriorityQueue = keyTimestampPriorityQueue.get(key);
		synchronized (thisKeyPriorityQueue) {
			thisKeyPriorityQueue.poll();
			thisKeyPriorityQueue.notifyAll();
		}
	}
	
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
                String consistency = map.get("consistency"); // strong or eventual
                Integer region = Integer.parseInt(map.get("region"));
                Long timestamp = Long.parseLong(map.get("timestamp"));
                
                /* TODO: Add code here to handle the put request
                     Remember to use the explicit timestamp if needed! */
                Thread thread = new Thread(new Runnable() {
					
					@Override
					public void run() {
						
		                log(String.format("%s processes PUT key: %s, value: %s, timestamp: %d, consistency: %s", Thread.currentThread().getName(), key, value, timestamp, consistency));
		                if ((consistency != null)  && consistency.equals("strong")) {
		                	log(Thread.currentThread().getName() + " strong PUT");
		                	acquireLock(key, timestamp);
		                	synchronized (workingGetNumber) {
		                		if (!workingGetNumber.containsKey(key)) {
		                			workingGetNumber.put(key, new AtomicInteger(0));
		                		}
		                	}
		                	while (true) {
		                		synchronized (workingGetNumber.get(key)) {
		                			if (workingGetNumber.get(key).get() > 0) {
		                				try {
		                					workingGetNumber.get(key).wait(); // wait for processing GET
		                				} catch (InterruptedException e) {
		                					e.printStackTrace();
		                				}
		                			}
		                			else break;
		                		}
		                	}
		                	miniDataStorage.put(key, value);
		                }
		                else if ((consistency != null) && consistency.equals("eventual")) {
		                	log(Thread.currentThread().getName() + " eventual PUT");
		                	
		                	if (!keyLatestTimeStamp.containsKey(key)) {
	                			keyLatestTimeStamp.put(key, 0L);
	                		}
	                		// else do nothing

	                		if (keyLatestTimeStamp.get(key).compareTo(timestamp) < 0) {
	                			miniDataStorage.put(key, value);
	                			keyLatestTimeStamp.put(key, timestamp);
	                		}
		                }
		                
		                // IMPORTANT: block coordinator!
		                String response = "stored";
		                req.response().putHeader("Content-Type", "text/plain");
		                req.response().putHeader("Content-Length",
		                        String.valueOf(response.length()));
		                req.response().end(response);
		                req.response().close();
		                
		                if (consistency.equals("strong"))
		                	releaseLock(key, timestamp);
					}
				}, THREAD_NAME_PREFIX + numberOfThreads.getAndIncrement());
                thread.start();
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
                
                Thread thread = new Thread(new Runnable() {
					
					@Override
					public void run() {
						log(String.format("%s processes GET key: %s, timestamp: %d, consistency: %s", Thread.currentThread().getName(), key, timestamp, consistency));
						String value = null;
						if ((consistency != null)  && consistency.equals("strong")) {
							log((Thread.currentThread().getName() + " strong GET"));
							acquireLock(key, timestamp);
							synchronized (workingGetNumber) {
								if (!workingGetNumber.containsKey(key)) {
									workingGetNumber.put(key, new AtomicInteger(0));
								}
							}
							workingGetNumber.get(key).incrementAndGet();
							releaseLock(key, timestamp);
							
							synchronized (miniDataStorage) {
								if (miniDataStorage.containsKey(key)) {
									value = miniDataStorage.get(key);
								}
							}
							workingGetNumber.get(key).decrementAndGet();
							synchronized (workingGetNumber.get(key)) {
								workingGetNumber.get(key).notifyAll(); // wake up the waiting PUT
							}
						}
						else if ((consistency != null)  && consistency.equals("eventual")) {
							log((Thread.currentThread().getName() + " eventual GET"));
							synchronized (miniDataStorage) {
								if (miniDataStorage.containsKey(key)) {
									value = miniDataStorage.get(key);
								}
							}
						}
						
		                String response = "";
		                if (value == null) {
		                	response = "null";
		                }
		                else response = value;
		                req.response().putHeader("Content-Type", "text/plain");
		                if (response != null)
		                    req.response().putHeader("Content-Length",
		                            String.valueOf(response.length()));
		                req.response().end(response);
		                req.response().close();
					}
				}, THREAD_NAME_PREFIX + numberOfThreads.getAndIncrement());
                
                thread.start();
            }
        });
        // Clears this stored keys. Do not change this
        routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                /* TODO: Add code to here to flush your datastore. This is MANDATORY */
            	log("reset");
            	miniDataStorage.clear();
            	numberOfThreads.set(0);
            	keyLatestTimeStamp.clear();
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
                
                Thread workThread = new Thread(new Runnable() {
					
					@Override
					public void run() {
		                log(String.format("%s processes AHEAD key = %s, timestamp = %d", Thread.currentThread().getName(), key, timestamp));
		                acquireLock(key, timestamp);
		                Integer locker;
		                synchronized (aheadCompleteLock) {
		                	if (!aheadCompleteLock.containsKey(key)) {
		                		aheadCompleteLock.put(key, new Integer(0));
		                	}
		                	locker = aheadCompleteLock.get(key);
		                }
		                synchronized (locker) {
		                	try {
								locker.wait();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
		                
		                releaseLock(key, timestamp);
					}
				}, THREAD_NAME_PREFIX + numberOfThreads.getAndIncrement());

                workThread.start();
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
                
                Thread thread = new Thread(new Runnable() {
					public void run() {
		                log(String.format("%s processes COMPLETE key = %s, timestamp = %d", Thread.currentThread().getName(), key, timestamp));
		                acquireLock(key, timestamp);
		                synchronized (aheadCompleteLock.get(key)) {
		                	aheadCompleteLock.get(key).notifyAll();
						}
		                releaseLock(key, timestamp);
					}
				}, THREAD_NAME_PREFIX + numberOfThreads.getAndIncrement());
                thread.start();
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

