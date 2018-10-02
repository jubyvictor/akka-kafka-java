##Akka - Kafka producer consumer POC

This is a simple project to demonstrate how to use Akka's Java API to consume messages off a Kafka topic, process it and emit it to
another kafka topic.

The pre-requisites are having a local kafka / docker environment setup. You can either choose to run docker or kafka on 
your machine directly.

The basic idea  is to have an event producer create a tiny message, push it to a topic, and an event processor listening
on that topic receive that message, do some processing on it and emit it to another queue.

There are two modules in the project event-producer which gives away what it does. It's a barebone Kafka producer. 
Nothing fancy happens there. You run the producer by running Main, if there's a kafka topic, it pumps through 
1 million messages to the topic and shuts itself down.

The event-processor is the module that does all the akka-kafka magic. You can run it by executing Main in the 
event-processor. The EventProcessor Actor gets messages from the kafka topic and requests the BlobReader actor to read a
file off the disk and returns its content. The blob reader has a random number generator that triggers an IOException if
the random no: generated is 7. 

This is still a work in progress and I plan to update the project with mode akka goodness as I build more expertise in 
Akka.

   