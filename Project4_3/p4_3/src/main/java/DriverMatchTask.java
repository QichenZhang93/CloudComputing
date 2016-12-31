package com.cloudcomputing.samza.pitt_cabs;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

    /* Define per task state here. (kv stores etc) */
    private double MAX_MONEY = 100.0;
    private KeyValueStore<String, HashMap<String, String>> KVStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize (maybe the kv stores?)
        KVStore = (KeyValueStore<String, HashMap<String, String>>) context.getStore("driver-loc");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        // The main part of your code. Remember that all the messages for a
        // particular partition
        // come here (somewhat like MapReduce). So for task 1 messages for a
        // blockId will arrive
        // at one task only, thereby enabling you to do stateful stream
        // processing.
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
	    // Handle Driver Location messages
            HandleDriverLocation((Map<String, Object>) envelope.getMessage());

        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
	    // Handle Event messages
            HandleEvent((Map<String, Object>) envelope.getMessage(), collector);

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        // this function is called at regular intervals, not required for this
        // project
    }

    String GenKey(Integer blockId, Integer driverId) {
        return blockId.toString() + ":" + driverId.toString();
    }

    HashMap<String, String> GetDriverFromKVStore(Integer blockId, Integer driverId) {
        HashMap<String, String> driver = KVStore.get(GenKey(blockId, driverId));
        if (driver == null)
            driver = new HashMap<>();
        return driver;
    }

    void HandleDriverLocation(Map<String, Object> driverLocation) {
        Integer blockId = (Integer) driverLocation.get("blockId");
        Integer driverId = (Integer) driverLocation.get("driverId");
        Double latitude = (Double) driverLocation.get("latitude");
        Double longitude = (Double) driverLocation.get("longitude");
        HashMap<String, String> driver = GetDriverFromKVStore(blockId, driverId);
        driver.put("latitude", latitude.toString());
        driver.put("longitude", longitude.toString());
        KVStore.put(GenKey(blockId, driverId), driver);
    }

    void HandleEvent(Map<String, Object> event, MessageCollector collector) {
        String type = (String) event.get("type");
        if (type.equals("LEAVING_BLOCK")) {
            Integer driverId = (Integer) event.get("driverId");
            Integer blockId = (Integer) event.get("blockId");
            KVStore.delete(blockId.toString() + ":" + driverId.toString());
        }
        else if (type.equals("ENTERING_BLOCK")) {
            Integer driverId = (Integer) event.get("driverId");
            Integer blockId = (Integer) event.get("blockId");
            Double latitude = (Double) event.get("latitude");
            Double longitude = (Double) event.get("longitude");
            String gender = (String) event.get("gender");
            Double rating = (Double) event.get("rating");
            Integer salary = (Integer) event.get("salary");
            String status = (String) event.get("status");

            HashMap<String, String> driver = GetDriverFromKVStore(blockId, driverId);
            driver.put("latitude", latitude.toString());
            driver.put("longitude", longitude.toString());
            driver.put("gender", gender);
            driver.put("rating", rating.toString());
            driver.put("salary", salary.toString());
            driver.put("status", status);
            KVStore.put(GenKey(blockId, driverId), driver);

        }
        else if (type.equals("RIDE_COMPLETE")) {
            Integer driverId = (Integer) event.get("driverId");
            Integer blockId = (Integer) event.get("blockId");
            Double latitude = (Double) event.get("latitude");
            Double longitude = (Double) event.get("longitude");
            String gender = (String) event.get("gender");
            Double rating = (Double) event.get("rating");
            Integer salary = (Integer) event.get("salary");

            HashMap<String, String> driver = GetDriverFromKVStore(blockId, driverId);

            driver.put("latitude", latitude.toString());
            driver.put("longitude", longitude.toString());
            driver.put("gender", gender);
            driver.put("rating", rating.toString());
            driver.put("salary", salary.toString());
            KVStore.put(GenKey(blockId, driverId), driver);

        }
        else if (type.equals("RIDE_REQUEST")) {
            Integer clientId = (Integer) event.get("clientId");
            Integer blockId = (Integer) event.get("blockId");
            Double latitude = (Double) event.get("latitude");
            Double longitude = (Double) event.get("longitude");
            String gender_preference = (String) event.get("gender_preference");
            // search for good driver
            KeyValueIterator<String, HashMap<String, String>> itr = KVStore.range(blockId.toString() + ":", blockId.toString() + ";");
            Integer driverId = 0;
            Double currentScore = 0.0;
            while (itr.hasNext()) {
                try {
                    Entry<String, HashMap<String, String>> entry = itr.next();

                    Map<String, String> driver = entry.getValue();
                    Double driverLatitude = Double.valueOf(driver.get("latitude"));

                    Double driverLongitude = Double.valueOf(driver.get("longitude"));
                    String driverGender = driver.get("gender");
                    Double rating = Double.valueOf(driver.get("rating"));
                    Integer salary = Integer.valueOf(driver.get("salary"));

                    String status = driver.get("status");
                    if (!"AVAILABLE".equals(status)) continue;

                    Double distanceScore = Math.pow(Math.E, -1 * GetDistance(latitude, longitude, driverLatitude, driverLongitude));
                    Double ratingScore = rating / 5.0;
                    Double salaryScore = 1 - salary / 100.0;
                    Double genderScore = (gender_preference.equals(driverGender) ? 1.0 : 0.0);
                    if (gender_preference.equals("N"))
                        genderScore = 1.0;

                    Double matchScore = distanceScore * 0.4 + genderScore * 0.2 + ratingScore * 0.2 + salaryScore * 0.2;
                    if (matchScore >= currentScore) {
                        driverId = Integer.valueOf(entry.getKey().split(":")[1]);
                        currentScore = matchScore;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            itr.close();
            KVStore.delete(GenKey(blockId, driverId));
            HashMap<String, Integer> result = new HashMap<>();
            result.put("clientId", clientId);
            result.put("driverId", driverId);
            collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, result));
        }
    }

    Double GetDistance(Double x1, Double y1, Double x2, Double y2) {
        return Math.sqrt(Math.pow(x1 - x2, 2.0) + Math.pow(y1 - y2, 2.0));
    }

}
