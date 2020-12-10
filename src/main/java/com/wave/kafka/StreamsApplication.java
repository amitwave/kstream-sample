package com.wave.kafka;

import java.util.function.Function;

import com.wave.kafka.model.Preference;
import com.wave.kafka.model.User;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

@SpringBootApplication
public class StreamsApplication {

	public static void main(String[] args) {
		SpringApplication.run(StreamsApplication.class, args);
	}



}
