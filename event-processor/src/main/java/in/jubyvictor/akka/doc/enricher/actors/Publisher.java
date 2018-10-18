package in.jubyvictor.akka.doc.enricher.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Publisher extends AbstractLoggingActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final KafkaProducer kafkaProducer;

    public static class Publish {
    }


    public Publisher(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public static Props props(KafkaProducer kafkaProducer) {
        return Props.create(Publisher.class, () -> new Publisher(kafkaProducer));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(BlobReader.Blob.class, m -> {
            this.handlePublish(m.getResult());
        }).build();
    }


    void handlePublish(byte[] bytes) {

        this.kafkaProducer.send(new ProducerRecord<>(getTopic(bytes), getKey(bytes), bytes));
    }

    /*
    The logic here can be conditional. For e.g. we can choose the topic based on some field in the bytes
    */

    String getTopic(byte[] bytes) {
        return "output-topic";
    }

    String getKey(byte[] bytes) {
        return "key";
    }


    //Lifecyle methods
    @Override
    public void preStart() throws Exception {
        log.info("Starting up Publisher");
    }

    @Override
    public void postStop() throws Exception {
        log.info("Shutting down Publisher");
    }


}
