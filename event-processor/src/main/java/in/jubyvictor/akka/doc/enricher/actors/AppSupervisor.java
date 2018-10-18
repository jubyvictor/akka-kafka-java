package in.jubyvictor.akka.doc.enricher.actors;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.RoundRobinPool;
import in.jubyvictor.akka.doc.enricher.KafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;

public class AppSupervisor extends AbstractActorWithTimers {


    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final ActorSystem system;
    private final KafkaConfig kafkaConfig;
    private KafkaConsumer<String,byte[]> kafkaConsumer;
    private KafkaProducer<String,byte[]> kafkaProducer;

    private ActorRef evtPxr;
    private ActorRef blobReader;
    private ActorRef poller;
    private ActorRef publisher;

    public AppSupervisor(ActorSystem system,KafkaConfig kafkaConfig){
        this.system = system;
        this.kafkaConfig=kafkaConfig;
    }

    //Message to bootstrap the application.
    public static class Bootstrap{
    }

    //Message indicating a Kafka infra failure.
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
                    this.poller.tell(new Poller.Stop(), getSelf());
                })
                .build();
    }


    void bootstrap() {

        Properties consumerConfig = new Properties();
        //TODO read from the kafka config
        consumerConfig.put("client.id", "default-client");
        consumerConfig.put("group.id", "foo");
        consumerConfig.put("bootstrap.servers", "localhost:9092");

        Properties producerConfig = new Properties();


        initializeKafkaConnections(consumerConfig,producerConfig);

        //Children of supervisor.

        //Handles reading a blob from the disk , can be an external system like S3.
        this.blobReader = this.getContext().actorOf(BlobReader.props().withRouter(new RoundRobinPool(10)), "routed-blob-reader");

        this.evtPxr = this.getContext().actorOf(EventProcessor.props(this.blobReader).withRouter(new RoundRobinPool(10)), "routed-event-processor");

        this.poller = this.getContext().actorOf(Poller.props(this.kafkaConsumer, this.evtPxr), "kafka-poller");

        this.publisher = this.getContext().actorOf(Publisher.props(this.kafkaProducer));


        log.info("Bootstrapped AppSupervisor ! "+ System.currentTimeMillis());
    }

    private void initializeKafkaConnections(Properties consumerConfig, Properties producerConfig) {
        //Receives message from the incoming topic
        this.kafkaConsumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer());
        this.kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getInputTopic()));

        //Producer that writes out to the output topic(s).
        this.kafkaProducer = new KafkaProducer<>(producerConfig, new StringSerializer(), new ByteArraySerializer());

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
