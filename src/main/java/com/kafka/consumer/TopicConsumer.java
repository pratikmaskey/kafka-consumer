package com.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.output.EndSystems;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Slf4j
@Service
public class TopicConsumer {

	private int count = 1;
	private ObjectMapper objectMapper = new ObjectMapper();

	@KafkaListener(topics = "messages", groupId = "group")
	public void consume(String key, String message) {

		try {

			EndSystems endSystems = new EndSystems();
			if(Strings.isNotEmpty(message)){
				endSystems = objectMapper.readValue(message, EndSystems.class);
			}

			Instant instant = Instant.ofEpochMilli(endSystems.getTimeStamp());
			System.out.println("Consumer1 Record Timestamp -"+Instant.ofEpochMilli(endSystems.getTimeStamp())+
					"Now Time -"+ Instant.now()+"Record -"+ endSystems.toString());
			Long millis = ChronoUnit.MILLIS.between(instant, Instant.now());
			Long timeToWait = 4*60*1000 - millis; //4 min delay
			System.out.println("Consumer1 Thread="+ Thread.currentThread().getName()+"  is sleeping for "+ timeToWait + " and will resume.");
			if(millis > 0 && millis < 4*60*1000)Thread.sleep(timeToWait);

		} catch (InterruptedException | IOException e) {
			throw new RuntimeException(e);
		}
		count++;
		System.out.println("Consumer1 Thread="+ Thread.currentThread().getName()+"Consumer1 Processed message = " + message +"  "+"count="+count
		+"Now time-"+ Instant.now());

	}

	@KafkaListener(topics = "messages", groupId = "group")
	public void consume1(String key, String message) {

		try {

			EndSystems endSystems = new EndSystems();
			if(Strings.isNotEmpty(message)){
				endSystems = objectMapper.readValue(message, EndSystems.class);
			}

			Instant instant = Instant.ofEpochMilli(endSystems.getTimeStamp());
			System.out.println("Consumer2 Record Timestamp -"+Instant.ofEpochMilli(endSystems.getTimeStamp())+
					" Now Time -"+ Instant.now()+"Record -"+ endSystems.toString());
			Long millis = ChronoUnit.MILLIS.between(instant, Instant.now());
			Long timeToWait = 4*60*1000 - millis; // 4 min delay
			System.out.println("Consumer2 Thread="+ Thread.currentThread().getName()+"  is sleeping for "+ timeToWait + " and will resume.");
			if(millis > 0 && millis < 4*60*1000)Thread.sleep(timeToWait);

		} catch (InterruptedException | IOException e) {
			throw new RuntimeException(e);
		}
		count++;
		System.out.println("Consumer2 Thread="+ Thread.currentThread().getName()+"Consumer2 Processed message = " + message +"  "+"count="+count
				+"Now time-"+ Instant.now());

	}

	@KafkaListener(topics = "messages", groupId = "group")
	public void consume2(String key, String message) {

		try {

			EndSystems endSystems = new EndSystems();
			if(Strings.isNotEmpty(message)){
				endSystems = objectMapper.readValue(message, EndSystems.class);
			}

			Instant instant = Instant.ofEpochMilli(endSystems.getTimeStamp());
			System.out.println("Consumer3 Record Timestamp -"+Instant.ofEpochMilli(endSystems.getTimeStamp())+
					" Now Time -"+ Instant.now()+"Record -"+ endSystems.toString());
			Long millis = ChronoUnit.MILLIS.between(instant, Instant.now());
			Long timeToWait = 4*60*1000 - millis; // 4 min delay
			System.out.println("Consumer3 Thread="+ Thread.currentThread().getName()+"  is sleeping for "+ timeToWait + " and will resume.");
			if(millis > 0 && millis < 4*60*1000)Thread.sleep(timeToWait);

		} catch (InterruptedException | IOException e) {
			throw new RuntimeException(e);
		}
		count++;
		System.out.println("Consumer3 Thread="+ Thread.currentThread().getName()+"Consumer3 Processed message = " + message +"  "+"count="+count
				+"Now time-"+ Instant.now());

	}

}