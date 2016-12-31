
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class DataProducer {

    public static void main(String[] args) {
        /*
            Task 1:
            In Task 1, you need to read the content in the tracefile we give to you, 
            and create two streams, feed the messages in the tracefile to different 
            streams based on the value of "type" field in the JSON string.

            Please note that you're working on an ec2 instance, but the streams should
            be sent to your samza cluster. Make sure you can consume the topics on the
            master node of your samza cluster before make a submission. 
        */
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.0.75:9092,172.31.13.245:9092,172.31.2.118:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "none");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("connections.max.idle.ms", 540000);

        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("tracefile"), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                JSONObject jsonObject = new JSONObject(line);
                Integer blockId = (Integer)jsonObject.get("blockId");
                String type = (String) jsonObject.get("type");
                if (type.equals("DRIVER_LOCATION")) {
                    producer.send(new ProducerRecord<>("driver-locations", blockId.toString(), line));
                }
                else {
                    producer.send(new ProducerRecord<>("events", blockId.toString(), line));
                }
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}