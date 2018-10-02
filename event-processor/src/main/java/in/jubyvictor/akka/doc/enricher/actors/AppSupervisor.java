package in.jubyvictor.akka.doc.enricher.actors;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.BackoffSupervisor;
import akka.routing.RoundRobinPool;
import in.jubyvictor.akka.doc.enricher.KafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class AppSupervisor extends AbstractActorWithTimers {


    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final ActorSystem system;
    private final KafkaConfig kafkaConfig;
    private KafkaConsumer<String,byte[]> kafkaConsumer;

    private ActorRef evtPxr;
    private ActorRef blobReader;
    private ActorRef kafkaPoller;

    public AppSupervisor(ActorSystem system,KafkaConfig kafkaConfig){
        this.system = system;
        this.kafkaConfig=kafkaConfig;
    }

    //Message to bootstrap application.
    public static class Bootstrap{
    }

    public static class KafkaFailure{
    }



    public static Props props(ActorSystem system,KafkaConfig kafkaConfig){
        return Props.create(AppSupervisor.class, ()-> new AppSupervisor(system,kafkaConfig));
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Bootstrap.class, m->{
                    this.bootstrap();
                })
                .match(KafkaFailure.class, m->{
                    log.info("GOT KAFKA FAILURE, SHUTTING DOWN KAFKA POLLER");
                    this.kafkaPoller.tell(new KafkaPoller.Stop(), getSelf());
                })
                .build();
    }


    void bootstrap() {

        Properties config = new Properties();
        config.put("client.id", "default-client");
        config.put("group.id", "foo");
        config.put("bootstrap.servers", "localhost:9092");

        connectToKafka(config);

        //Children of supervisor.
        this.blobReader = this.getContext().actorOf(BlobReader.props().withRouter(new RoundRobinPool(20)), "routed-blob-reader");
        this.evtPxr = this.getContext().actorOf(EventProcessor.props(this.blobReader).withRouter(new RoundRobinPool(40)), "routed-event-processor");

        this.kafkaPoller = this.getContext().actorOf(KafkaPoller.props(this.kafkaConsumer, this.evtPxr), "kafka-poller");


        log.info("Bootstrapped AppSupervisor ! "+ System.currentTimeMillis());
    }

    private void connectToKafka(Properties config) {
        kafkaConsumer = new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer());
        kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getInputTopic()));
    }


    @Override
    public void preStart() throws Exception {
        log.info("Starting up AppSupervisor");
    }

    @Override
    public void postStop() throws Exception {
        log.info("Shutting down AppSupervisor");
    }


}
