# Kafka delay message consumption 
 Adding time delay to kafka consumer using sleep()

### Guide

* Both the producers will be pushing message to "messages" topic.
* There are 3 consumers which will be consuming form messages topic.
* If "messages" topic has 3 partitions all 3 consumers will be running else few will be idle. 
