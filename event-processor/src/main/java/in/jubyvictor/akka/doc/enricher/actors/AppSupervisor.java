package in.jubyvictor.akka.doc.enricher.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import in.jubyvictor.akka.doc.enricher.KafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class AppSupervisor extends AbstractLoggingActor {


    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final ActorSystem system;
    private final KafkaConfig kafkaConfig;
    private KafkaConsumer<String,byte[]> kafkaConsumer;


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

                })
                .build();
    }


    void bootstrap() {

        Properties config = new Properties();
        config.put("client.id", "default-client");
        config.put("group.id", "foo");
        config.put("bootstrap.servers", "localhost:9092");

        connectToKafka(config);

        ActorRef blobRdr = this.system.actorOf(BlobReader.props(), "blob-reader");

        ActorRef evtPxr = this.system.actorOf(EventProcessor.props(this.kafkaConsumer, blobRdr), "event-processor");
        evtPxr.tell(new EventProcessor.Consume(), getSelf());

        log.info("Bootstrapped EventProcessor !");
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
