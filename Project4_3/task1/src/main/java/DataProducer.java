//package main.java;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.*;
import java.util.Properties;

public class DataProducer {

    private static Producer<Integer, String> setUpProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "174.129.76.29:9092"); // TODO change this to master node public IP
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<Integer, String>(props);
    }

    public static void main(String[] args) throws IOException {
        /*
            Task 1:
            In Task 1, you need to read the content in the tracefile we give to you,
            and create two streams, feed the messages in the tracefile to different
            streams based on the value of "type" field in the JSON string.

            Please note that you're working on an ec2 instance, but the streams should
            be sent to your samza cluster. Make sure you can consume the topics on the
            master node of your samza cluster before make a submission.
        */
        String tracePath = System.getenv("tracePath");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(tracePath)));
        String line;
        Producer<Integer, String> producer = setUpProducer();
        while (true) {
            line = reader.readLine();
            if (line == null) {
                break;
            }
            JSONObject jsonObject = new JSONObject(line);
            String type = jsonObject.getString("type");
            Integer blockId = jsonObject.getInt("blockId");
            if (type.equalsIgnoreCase("DRIVER_LOCATION")) {
                producer.send(new ProducerRecord<Integer, String>("driver-locations", blockId % 5, blockId, line));
            }
            else {
                producer.send(new ProducerRecord<Integer, String>("events", blockId % 5, blockId, line));
            }
        }
        System.out.println("END~~~");
        producer.close();
        reader.close();
    }
}
