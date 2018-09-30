package in.jubyvictor.akka.doc.enricher;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.jubyvictor.akka.doc.enricher.actors.AppSupervisor;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static java.util.logging.Level.SEVERE;

public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());



    public static void main(String[] args) {

        Options cliOptions = initOptions();


        try {
            Map<String, Object> parsedArgs = parseArgs(args, cliOptions);

            boolean argsOk = validateArgs(parsedArgs);

            if (argsOk) {



                //Init kafka configs
                Config producerConfig = readKafkaConfig("PRODUCER", parsedArgs);
                Config consumerConfig = readKafkaConfig("CONSUMER", parsedArgs);

                KafkaConfig kafkaConfig = new KafkaConfig(consumerConfig,producerConfig,"input","output-1");


                //Init actor system
                final ActorSystem actorSystem;actorSystem = ActorSystem.create("akka-doc-enricher");

                Runtime.getRuntime().addShutdownHook(new Thread(()->{ actorSystem.terminate();}));

                final ActorRef supervisor = actorSystem.actorOf(AppSupervisor.props(actorSystem,kafkaConfig));

                supervisor.tell(new AppSupervisor.Bootstrap(), ActorRef.noSender());

            }
        } catch (IllegalArgumentException ex) {
            printHelp(cliOptions);
            System.exit(0);
        } catch (ParseException | IOException ex) {
            LOG.log(SEVERE, ex.getMessage(), ex);
            System.exit(1);
        }


    }


    //Read the kafka configs.
    static Config readKafkaConfig(String type, Map<String, Object> parsedArgs) {
        Config conf = null;
        switch (type) {
            case "PRODUCER":
                conf = ConfigFactory.parseFile(new File("config/producer.conf"));
            break;
            case "CONSUMER":
                conf = ConfigFactory.parseFile(new File("config/consumer.conf"));
            break;
            default:
                break;
        }
        return conf;

    }


    //Helper methods related to arg parsing.

    private static Options initOptions() {
        Options options = new Options();
        options.addOption("config", true, "path to app_config.yaml");
        return options;
    }

    private static Map<String, Object> parseArgs(String[] args, Options options) throws ParseException, IOException {
        Map parsedArgs = new HashMap<String, Object>();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("config")) {
            String pathToConfigFile = cmd.getOptionValue("config");
            File configFile = new File(pathToConfigFile);
            if (!(configFile.exists() && configFile.canRead())) {
                LOG.log(SEVERE, "Could not find the config file, exiting " + pathToConfigFile);
                System.exit(1);
            } else {
                parsedArgs = mapper.readValue(configFile, parsedArgs.getClass());
            }
        } else {
            printHelp(options);
        }

        return parsedArgs;
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("config", options);
    }

    private static boolean validateArgs(Map<String, Object> parsedAgs) throws IllegalArgumentException {
        boolean argsAreOk = true;

        return argsAreOk;
    }

}