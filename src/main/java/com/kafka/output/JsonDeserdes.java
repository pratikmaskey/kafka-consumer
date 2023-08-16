package com.kafka.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonDeserdes implements Deserializer<EndSystems> {

	/*@Override
	public EndSystems deserialize(InputStream inputStream) throws IOException {

		ObjectMapper objectMapper = new ObjectMapper();

		return objectMapper.readValue(inputStream, EndSystems.class);
	}*/

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		Deserializer.super.configure(configs, isKey);
	}

	@Override
	public EndSystems deserialize(String s, byte[] bytes) {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());
		try {
			return objectMapper.readValue(bytes, EndSystems.class);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public EndSystems deserialize(String topic, Headers headers, byte[] data) {
		return Deserializer.super.deserialize(topic, headers, data);
	}

	@Override
	public void close() {
		Deserializer.super.close();
	}
}
