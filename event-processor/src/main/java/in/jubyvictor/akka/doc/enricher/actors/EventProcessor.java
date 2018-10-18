package in.jubyvictor.akka.doc.enricher.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.PatternsCS;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static  in.jubyvictor.akka.doc.enricher.actors.BlobReader.Blob;
import static  in.jubyvictor.akka.doc.enricher.actors.BlobReader.ReadBlob;
import static  in.jubyvictor.akka.doc.enricher.actors.Publisher.Publish;


public class EventProcessor extends AbstractLoggingActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final String uuid = UUID.randomUUID().toString();

    private final ActorRef blobReader;
    private final ActorRef publisher;

    private final int READ_TIMEOUT_MS = 3000;


    public static class HandleMessage {
    }


    /**
     *
     *
     * @param blobReader
     * @param publisher
     */
    public EventProcessor(ActorRef blobReader, ActorRef publisher) {
        this.blobReader = blobReader;
        this.publisher = publisher;
    }

    public static Props props(ActorRef blobReader, ActorRef publisher) {
        return Props.create(EventProcessor.class, () -> new EventProcessor(blobReader, publisher));
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(HandleMessage.class, m -> {
                    this.handleMessage();
                })
                .build();
    }


    void handleMessage() {
        // Asks the blob reader to read the file from the file store, waits for the response, creates a message
        // from the response and pushes it downstream to a topic.
        CompletionStage<Object> future = PatternsCS.ask(this.blobReader, new ReadBlob(""), READ_TIMEOUT_MS);
        future.thenAccept(b -> this.publisher.tell(new Publish(((Blob)b).getResult()), this.getSelf()));

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
