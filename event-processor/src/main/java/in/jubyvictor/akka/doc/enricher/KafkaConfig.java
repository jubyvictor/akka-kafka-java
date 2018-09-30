package in.jubyvictor.akka.doc.enricher;

import com.typesafe.config.Config;

public class KafkaConfig {

    private final Config producerConf;
    private final Config consumerConf;
    private final String inputTopic;
    private final String outputTopic;

    private KafkaConfig(){
        producerConf=null;
        consumerConf=null;
        inputTopic=null;
        outputTopic=null;
    }

    public KafkaConfig(Config consumerConf, Config producerConf,  String inputTopic, String outputTopic){
        this.consumerConf = consumerConf;
        this.producerConf = producerConf;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    public Config getConsumerConf() {
        return consumerConf;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public Config getProducerConf() {
        return producerConf;
    }
}
