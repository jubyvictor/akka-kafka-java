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

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final String uuid = UUID.randomUUID().toString();

    private final ActorRef blobReader;


    public static class HandleMessage{}


    public EventProcessor(ActorRef blobReader) {
        this.blobReader = blobReader;
    }

    public static Props props(ActorRef blobReader) {
        return Props.create(EventProcessor.class, () -> new EventProcessor(blobReader));
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(HandleMessage.class, m -> {
                    this.handleMessage();
                })
                .match(BlobReader.Blob.class, m -> {
                    this.handleBlob(m.getResult());
                })
                .build();
    }



    void handleMessage() {
        //log.info("Handling message");
        this.blobReader.tell(new BlobReader.ReadBlob(""), getSelf());

    }



    void handleBlob(byte[] bytes) {
        log.info("PXD message");
        //log.debug(String.format("Got %d bytes in response %s",bytes.length, uuid));
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
