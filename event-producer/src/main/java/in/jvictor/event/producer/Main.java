package in.jvictor.event.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put("client.id","default-producer");
        config.put("bootstrap.servers", "localhost:9092");
        config.put("acks", "all");

        KafkaProducer<String,byte[]> producer = new KafkaProducer<>(config, new StringSerializer(), new ByteArraySerializer());

        long start = System.currentTimeMillis();

        long maxMessages = 1000000;
        long i=0;
        for(; i< maxMessages; i++){

            final ProducerRecord<String, byte[]> record = new ProducerRecord<>("input",
                    String.format("key-%d",i%2),
                    String.format("val-%d",i).getBytes());

            Future<RecordMetadata> future = producer.send(record, (RecordMetadata metadata, Exception e) -> {
                    if (e != null)
                        log.warn("Send failed for record {}", record, e);
                }
            );

        }

        long end = System.currentTimeMillis();

        log.info(String.format("Pumped %d messages in %d seconds", i, (end-start)/1000));


    }
}
