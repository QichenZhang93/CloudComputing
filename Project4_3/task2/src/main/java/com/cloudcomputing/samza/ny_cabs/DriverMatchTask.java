package com.cloudcomputing.samza.ny_cabs;

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
import java.util.*;

/*
*
*
*
*
*
*
*
*
*
*
*
*
*
*
* */
/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

    /* Define per task state here. (kv stores etc) */
    private static final double MAX_MONEY = 100.0;
    private KeyValueStore<String, Map<String, String>> KVStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize (maybe the kv stores?)
        KVStore = (KeyValueStore<String, Map<String, String>>) context.getStore("driver-loc");
        KVStore.flush();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {

        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            processDriverLocation((Map<String, Object>) envelope.getMessage());

        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            processEvent((Map<String, Object>) envelope.getMessage(), collector);

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        // this function is called at regular intervals, not required for this
        // project
    }

    private String generateKey(Integer blockId, Integer driverId) {
        return blockId.toString() + ":" + driverId.toString();
    }

    private Integer getDriverIdFromKey(String key) {
        return Integer.valueOf(key.split(":")[1]);
    }

    private Map<String, String> getDriverFromKVStore(Integer blockId, Integer driverId) {
        Map<String, String> driver = KVStore.get(generateKey(blockId, driverId));
        if (driver == null)
            driver = new LinkedHashMap<>();
        return driver;
    }

    private void processDriverLocation(Map<String, Object> driverLocation) {
        Integer blockId = (Integer) driverLocation.get(Keys.BLOCK_ID);
        Integer driverId = (Integer) driverLocation.get(Keys.DRIVER_ID);
        Double latitude = (Double) driverLocation.get(Keys.LATITUDE);
        Double longitude = (Double) driverLocation.get(Keys.LONGITUDE);
        Map<String, String> driver = getDriverFromKVStore(blockId, driverId);
        driver.put(Keys.LATITUDE, latitude.toString());
        driver.put(Keys.LONGITUDE, longitude.toString());
        KVStore.put(generateKey(blockId, driverId), driver);
    }

    class DriverRatingPair {
        Integer id;
        Double rating;
        DriverRatingPair(Integer id, Double rating) {
            this.id = id;
            this.rating = rating;
        }
    }

    private void processLeaderBoard(Integer blockId, Integer driverId, MessageCollector collector) {
        KeyValueIterator<String, Map<String, String>> itr = KVStore.range(blockId.toString() + ":", blockId.toString() + ";");
        PriorityQueue<DriverRatingPair> ratingPQ = new PriorityQueue<>(3, new Comparator<DriverRatingPair>() {
            @Override
            public int compare(DriverRatingPair o1, DriverRatingPair o2) {
                if (o1.rating.equals(o2.rating)) {
                    return o2.id.compareTo(o1.id);
                }
                else {
                    return o1.rating.compareTo(o2.rating);
                }
            }
        });
        while (itr.hasNext()) {
            Entry<String, Map<String, String>> entry = itr.next();
            Integer dId = getDriverIdFromKey(entry.getKey());
            if (entry.getValue().get(Keys.RATING) == null) {
                continue;
            }

            Double rating = Double.valueOf(entry.getValue().get(Keys.RATING));
            ratingPQ.add(new DriverRatingPair(dId, rating));
            while (ratingPQ.size() > 3) {
                ratingPQ.poll();
            }
        }
        Map<String, Object> res = new LinkedHashMap<>();
        res.put(Keys.BLOCK_ID, blockId);
        res.put(Keys.DRIVER_ID, driverId);

        LinkedList<Integer> ranks = new LinkedList<>();
        while (!ratingPQ.isEmpty()) {
            ranks.addFirst(ratingPQ.poll().id);
        }
        res.put("ranks", ranks);
        collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.LEADERBOARD_STREAM, res));
    }

    private void processEvent(Map<String, Object> event, MessageCollector collector) {
        String type = (String) event.get(Keys.TYPE);
        Integer blockId = (Integer) event.get(Keys.BLOCK_ID);

        if (type.equals(EventType.RIDE_REQUEST)) {
            Integer clientId = (Integer) event.get(Keys.CLIEND_ID);
            Double latitude = (Double) event.get(Keys.LATITUDE);
            Double longitude = (Double) event.get(Keys.LONGITUDE);
            String gender_preference = (String) event.get(Keys.GENDER_PREF);

            // search for good driver
            KeyValueIterator<String, Map<String, String>> itr = KVStore.range(blockId.toString() + ":", blockId.toString() + ";");
            Integer driverId = 0;
            Double maxMatchScore = 0.0;

            while (itr.hasNext()) {
                try {
                    Entry<String, Map<String, String>> entry = itr.next();

                    Map<String, String> driver = entry.getValue();

                    if (driver.get(Keys.RATING) == null) {
                        continue;
                    }
                    Double driverLatitude = Double.valueOf(driver.get(Keys.LATITUDE));
                    Double driverLongitude = Double.valueOf(driver.get(Keys.LONGITUDE));
                    String driverGender = driver.get(Keys.GENDER);
                    String status = driver.get(Keys.STATUS);

                    if (!"AVAILABLE".equals(status)) continue;

                    Double rating = Double.valueOf(driver.get(Keys.RATING));
                    Integer salary = Integer.valueOf(driver.get(Keys.SALARY));

                    Double distanceScore = Math.pow(Math.E, -1 * getDis(latitude, longitude, driverLatitude, driverLongitude));
                    Double ratingScore = rating / 5.0;
                    Double salaryScore = 1 - salary / MAX_MONEY;
                    Double genderScore = 0.0;
                    if (gender_preference.equalsIgnoreCase("N")
                            || gender_preference.equalsIgnoreCase(driverGender)) {
                        genderScore = 1.0;
                    }

                    Double matchScore = distanceScore * 0.4 + genderScore * 0.2 + ratingScore * 0.2 + salaryScore * 0.2;
                    if (matchScore > maxMatchScore) {
                        driverId = getDriverIdFromKey(entry.getKey());
                        maxMatchScore = matchScore;
                    }
                } catch (Exception e) {
                    /* if exceptions like null point exception happens,
                    this means no attribute is found and we should skip the record */
                    e.printStackTrace();
                }
            }
            itr.close();
            KVStore.delete(generateKey(blockId, driverId));
            HashMap<String, Integer> result = new HashMap<>();
            result.put(Keys.CLIEND_ID, clientId);
            result.put(Keys.DRIVER_ID, driverId);
            collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, result));
        }
        else {
            Double latitude = (Double) event.get(Keys.LATITUDE);
            Double longitude = (Double) event.get(Keys.LONGITUDE);
            Integer driverId = (Integer) event.get(Keys.DRIVER_ID);

            if (type.equals(EventType.LEAVING_BLOCK)) {
                KVStore.delete(generateKey(blockId, driverId));

            } else if (type.equals(EventType.ENTERING_BLOCK)) {
                String gender = (String) event.get(Keys.GENDER);
                Double rating = (Double) event.get(Keys.RATING);
                Integer salary = (Integer) event.get(Keys.SALARY);
                String status = (String) event.get(Keys.STATUS);

                Map<String, String> driver = getDriverFromKVStore(blockId, driverId);
                driver.put(Keys.LATITUDE, latitude.toString());
                driver.put(Keys.LONGITUDE, longitude.toString());
                driver.put(Keys.GENDER, gender);
                driver.put(Keys.RATING, rating.toString());
                driver.put(Keys.SALARY, salary.toString());
                driver.put(Keys.STATUS, status);
                KVStore.put(generateKey(blockId, driverId), driver);

            } else if (type.equals(EventType.RIDE_COMPLETE)) {
                String gender = (String) event.get(Keys.GENDER);
                Double rating = (Double) event.get(Keys.RATING);
                Integer salary = (Integer) event.get(Keys.SALARY);
                Double userRating = (Double) event.get(Keys.U_RATING);

                Map<String, String> driver = getDriverFromKVStore(blockId, driverId);
                driver.put(Keys.LATITUDE, latitude.toString());
                driver.put(Keys.LONGITUDE, longitude.toString());
                driver.put(Keys.GENDER, gender);
                driver.put(Keys.RATING, String.valueOf((rating + userRating) / 2.0)); // avg of rating & user rating
                driver.put(Keys.SALARY, salary.toString());
                driver.put(Keys.STATUS, "AVAILABLE");
                KVStore.put(generateKey(blockId, driverId), driver);

                processLeaderBoard(blockId, driverId, collector);

            }
        }
    }

    private Double getDis(Double x1, Double y1, Double x2, Double y2) {
        return Math.sqrt(Math.pow(x1 - x2, 2.0) + Math.pow(y1 - y2, 2.0));
    }

}
