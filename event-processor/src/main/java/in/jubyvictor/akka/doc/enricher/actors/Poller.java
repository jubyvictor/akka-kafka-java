package in.jubyvictor.akka.doc.enricher.actors;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;


//Uses a timer to schedule periodic polls on the kafka broker.
//Messages received during polls are sent off the event processor.
public class Poller extends AbstractActorWithTimers {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final ActorRef eventProcessor;
    private final KafkaConsumer kafkaConsumer;
    private static Object TICK_KEY = "TICK";

    private static final String uuid = UUID.randomUUID().toString();

    private static final int POLL_INTERVAL_MS=5;
    private static final int POLL_TIMEOUT_MS=100;
    private long startTime = System.currentTimeMillis();
    private AtomicLong count = new AtomicLong(0);


    public static Props props(KafkaConsumer kafkaConsumer, ActorRef eventProcessor){
        return Props.create(Poller.class, ()-> new Poller(kafkaConsumer,eventProcessor));
    }



    public Poller(KafkaConsumer kafkaConsumer, ActorRef evtProcessor){
        //Polls kafka every POLL_INTERVAL_MS
        this.getTimers().startSingleTimer(TICK_KEY, new InitialTick(), Duration.ofMillis(POLL_INTERVAL_MS));
        this.kafkaConsumer = kafkaConsumer;
        this.eventProcessor = evtProcessor;
    }


    //Protocol
    private static final class InitialTick {
    }

    private static final class Tick {
    }

    public static class Stop {
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(InitialTick.class, m -> {
                    getTimers().startPeriodicTimer(TICK_KEY, new Tick(), Duration.ofMillis(POLL_INTERVAL_MS));
                })
                .match(Tick.class, m -> {
                    this.publish();
                })
                .match(Stop.class, m -> {
                    log.info(String.format("Stopping Kafka poller %s.", uuid));
                    this.stop();
                }).build();
    }

    void stop() {
        log.info(String.format("Stopping kafka poller %s", uuid));
        this.getContext().stop(getSelf());
    }

    void publish() {
        //log.info(String.format("Kafka Poller %s is polling...", uuid));
        try {
            ConsumerRecords records = kafkaConsumer.poll(POLL_TIMEOUT_MS);
            if(records.count() == 0) {
                log.info(String.format("Start = %d, Now = %d, PXD %d", startTime, System.currentTimeMillis(), count.get()));
            }
            records.forEach(rec -> {
                //Publish polled messages to routed event processor
                this.eventProcessor.tell(new EventProcessor.HandleMessage(), getSelf());
                count.getAndIncrement();
            });
            //Commit offset on no poll errors.
            kafkaConsumer.commitAsync();
        } catch (IllegalStateException | KafkaException e) {
            log.error(String.format("Consumer poll error : %s", e.getMessage()));
            getContext().getParent().tell(new AppSupervisor.KafkaFailure(), getSelf());
        }
    }

}
