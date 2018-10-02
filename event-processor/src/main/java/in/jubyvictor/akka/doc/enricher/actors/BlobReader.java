package in.jubyvictor.akka.doc.enricher.actors;


import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

//Actor that returns the content of a json file.
//Throws IOException if the random number is 7 to simulate unexpected exceptions.
public class BlobReader extends AbstractLoggingActor {

    private Random rand = new Random();
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private byte[] cache;


    @Override
    public void preStart() throws Exception {
        log.info("Starting up BlobReader");
    }

    @Override
    public void postStop() throws Exception {
        log.info("Shutting down BlobReader");
    }

    //Protocol classes
    public static class ReadBlob{
        private final String requestId;

        public ReadBlob(String requestId){
            this.requestId = requestId;
        }
        public String getRequestId(){ return  this.requestId;}
    }

    public static class Blob{
        private final byte[] result;
        public Blob(byte[] result){
            this.result = result;
        }
        public byte[] getResult(){ return  this.result;}
    }

    public static Props props(){
        return Props.create(BlobReader.class, ()-> new BlobReader());
    }

    BlobReader(){
    }




    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadBlob.class, m ->{
                    //log.info("Reading blob..");
                    getSender().tell(new Blob(readBlob(m.requestId)),getSelf());
                })
                .build();
    }


    byte[] readBlob(String requestId) throws IOException {
        int r = rand.nextInt(10);
        if( r == -1){
            throw new IOException();
        }
        else{
            if (cache == null) {
                cache = Files.readAllBytes(Paths.get("event-processor/config/app_config.yaml"));
            }
            return cache;
        }
    }

}
