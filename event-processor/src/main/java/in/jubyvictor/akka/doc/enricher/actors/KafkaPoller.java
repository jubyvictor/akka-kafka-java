package in.jubyvictor.akka.doc.enricher.actors;

import akka.actor.AbstractLoggingActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;


//Uses a time to schedule periodic polls on the kafka broker.
//Messages received during polls are sent off the event processor.
public class KafkaPoller extends AbstractLoggingActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private final KafkaConsumer kafkaConsumer;


    public KafkaPoller(KafkaConsumer kafkaConsumer){
        this.kafkaConsumer = kafkaConsumer;
    }


    //Protocol
    private static final class InitialTick {
    }

    private static final class Tick {
    }

    public static class Stop {
    }

    public static class Consume {
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Consume.class, m -> {
                    this.consume();
                })
                .match(Stop.class, m -> {
                    log.info("Stopping Kafka poller.");
                    this.stop();
                }).build();
    }

    void stop() {
        log.info("Stopping kafka poller ");
        this.getContext().stop(getSelf());
    }

    void consume() {
        try {
            ConsumerRecords records = kafkaConsumer.poll(100);

            records.forEach(rec -> {
                getContext().getSystem().actorOf(EventProcessor.)
            });
        } catch (IllegalStateException | KafkaException e) {
            log.error(String.format("Consumer poll error : %s", e.getMessage()));
            getContext().getParent().tell(new AppSupervisor.KafkaFailure(), getSelf());
        }
        kafkaConsumer.commitAsync();
    }

}
