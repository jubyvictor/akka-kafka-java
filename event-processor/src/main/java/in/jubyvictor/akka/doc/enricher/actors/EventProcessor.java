package in.jubyvictor.akka.doc.enricher.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static akka.pattern.Patterns.ask;


public class EventProcessor extends AbstractLoggingActor {


    private final KafkaConsumer kafkaConsumer;
    private final ActorRef blobReader;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch  = new CountDownLatch(1);
    Timeout pollTimeout = Timeout.apply(10, TimeUnit.SECONDS);





    public EventProcessor(KafkaConsumer kafkaConsumer, ActorRef reader){
        this.kafkaConsumer = kafkaConsumer;
        this.blobReader = reader;
    }

    public static Props props(KafkaConsumer kafkaConsumer, ActorRef blobReader){
        return Props.create(EventProcessor.class, () -> new EventProcessor(kafkaConsumer, blobReader));
    }

    //Protocol
    public static class Pause{}
    public static class Resume{}
    public static class Consume{}


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Consume.class, m->{
                    this.consume();
                })
                .match(BlobReader.Blob.class, m->{
                    this.handleBlob(m.getResult());
                })
                .match(Pause.class, m->{
                    this.pause();
                })
                .match(Resume.class, m->{
                    this.resume();
                })
                .build();
    }

    void pause(){
        log.info("Pausing event processor");
        shouldRun.set(false);
    }
    void resume(){
        log.info("Resuming event processor");
        shouldRun.set(true);
        this.consume();
    }

    void consume(){
        log.info(String.format("Starting consume at %d ",System.currentTimeMillis()));
        CompletableFuture.runAsync(() -> {
            while(shouldRun.get()){
                ConsumerRecords records = kafkaConsumer.poll(100);
                records.forEach(rec -> {
                    blobReader.tell(new BlobReader.ReadBlob(rec.toString()), getSelf());
                    //ask(blobReader,new BlobReader.ReadBlob(rec.toString()) , pollTimeout);
                });
                kafkaConsumer.commitAsync();
            }
        });
    }



    void handleBlob(byte[] bytes){
        log.info(String.format("Got %d bytes in response",bytes.length));
    }

    @Override
    public void preStart() throws Exception {
        log.info("Starting up EventProcessor");
    }

    @Override
    public void postStop() throws Exception {
        log.info("Shutting down EventProcessor");
    }


}
