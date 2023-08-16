package com.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.output.EndSystems;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class Producer1 {

	private static final String TOPIC = "messages";
	private static final String BOOTSTRAP_SERVERS = "localhost:9093";

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props);

		Integer count = 1;

		while (true) {

			//Date date = new Date();
			Random random = new Random();
			String ip = "192.168.0" + random.nextInt(254);
			EndSystems endSystems = EndSystems.builder().id(count++).ip(ip).type("XOS")
					.timeStamp(Instant.now().toEpochMilli()).build();
			ObjectMapper obj = new ObjectMapper();
			String jsonStr = null;
			try {
				jsonStr = obj.writeValueAsString(endSystems);
				ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, jsonStr);
				producer.send(record);
				//Thread.sleep(50000);
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		}
	}
}