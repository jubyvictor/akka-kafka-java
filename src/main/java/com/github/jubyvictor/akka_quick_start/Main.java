package com.github.jubyvictor.akka_quick_start;


import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.stream.ActorMaterializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static java.util.logging.Level.*;

public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        Options cliOptions = initOptions();

        try{
            Map<String, Object> parsedArgs = parseArgs(args, cliOptions);

            boolean argsOk = validateArgs(parsedArgs);

            if(argsOk){

                Main self = new Main();

                //Init kafka
                self.initKafkaClient(parsedArgs);

                //Init Database
                self.initDatabase(parsedArgs);

                //Init actor system
                ActorSystem actorSystem = ActorSystem.create("akka-quick-start-server");
                final Http http = Http.get(actorSystem);
                final ActorMaterializer materializer = ActorMaterializer.create(actorSystem);



            }
        }
        catch(IllegalArgumentException ex){
            printHelp(cliOptions);
            System.exit(0);
        }
        catch(ParseException | IOException ex){
            LOG.log(SEVERE, ex.getMessage(), ex);
            System.exit(1);
        }

    }



    //Init the kafka client.
    void initKafkaClient(Map<String,Object> parsedArgs){

    }


    //Init the database client.
    void initDatabase(Map<String,Object> parsedArgs){

    }


    //Helper methods related to arg parsing.

    private static Options initOptions(){
        Options options = new Options();
        options.addOption("config", true, "path to app_config.yaml");
        return options;
    }

    private static Map<String,Object> parseArgs(String[] args, Options options) throws ParseException, IOException {
        Map parsedArgs = new HashMap<String, Object>();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse( options, args);
        if(cmd.hasOption("config")) {
            String pathToConfigFile = cmd.getOptionValue("config");
            File configFile = new File(pathToConfigFile);
            if(! (configFile.exists() && configFile.canRead()) ){
                LOG.log(SEVERE, "Could not find the config file, exiting "+pathToConfigFile);
                System.exit(1);
            }
            else{
                parsedArgs = mapper.readValue(configFile,parsedArgs.getClass());
            }
        }
        else {
            printHelp(options);
        }

        return  parsedArgs;
    }

    private static void printHelp(Options options){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "config", options );
    }

    private static boolean validateArgs(Map<String, Object> parsedAgs) throws IllegalArgumentException{
        boolean argsAreOk = false;

        return argsAreOk;
    }

}