package in.jubyvictor.akka.doc.enricher.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static akka.pattern.Patterns.ask;


public class EventProcessor extends AbstractLoggingActor {



    private final ActorRef blobReader;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final AtomicBoolean shouldRun = new AtomicBoolean(true);

    private final String uuid = UUID.randomUUID().toString();


    public EventProcessor(ActorRef reader) {
        this.blobReader = reader;
    }

    public static Props props() {
        return Props.create(EventProcessor.class, () -> new EventProcessor(kafkaConsumer, blobReader));
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(BlobReader.Blob.class, m -> {
                    this.handleBlob(m.getResult());
                })
                .build();
    }







    void handleBlob(byte[] bytes) {
        //log.info(String.format("Got %d bytes in response %s",bytes.length, uuid));
    }

    @Override
    public void preStart() throws Exception {
        log.info("Starting up EventProcessor :" + uuid);
    }

    @Override
    public void postStop() throws Exception {
        log.info("Shutting down EventProcessor :" + uuid);
    }


}
