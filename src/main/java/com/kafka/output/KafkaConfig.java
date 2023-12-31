package com.kafka.output;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

	@Bean
	public ConsumerFactory<String, String> consumerFactory(){
		Map<String,Object> map = new HashMap<>();
		map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9093");
		map.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_group");
		map.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 700000);
		map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		map.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 650000);
		map.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 700000);
		map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return new DefaultKafkaConsumerFactory<>(map);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String,String> kafkaListenerContainerFactory(){
		ConcurrentKafkaListenerContainerFactory<String,String> kafkaListenerContainerFactory= new ConcurrentKafkaListenerContainerFactory<>();
		kafkaListenerContainerFactory.setConsumerFactory(consumerFactory());
		return kafkaListenerContainerFactory;
	}
}
