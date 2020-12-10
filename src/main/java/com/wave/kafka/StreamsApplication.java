package com.wave.kafka;

import java.util.function.Function;

import com.wave.kafka.model.Preference;
import com.wave.kafka.model.User;
import com.wave.kafka.streams.splitter.UserProcessorBinding;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

@SpringBootApplication
public class StreamsApplication {

	public static void main(String[] args) {
		SpringApplication.run(StreamsApplication.class, args);
	}

	public static class UserDataSplitter {

		private final Serde<User> userSerde = new JsonSerde<>(User.class);

		private final  Serde<String> keySerde = new JsonSerde<>(String.class);


		@Bean
		@SuppressWarnings("unchecked")
		public Function<KStream<String, User>, KStream<?, User>[]> process() {
			Predicate<String, String> isTea = (k, v) -> v.contains(Preference.TEA.name());
			Predicate<String, String> isCoffee = (k, v) -> v.contains(Preference.COFFEE.name());

			System.out.println("User Splitterby tea/coffee");

			return kstream -> kstream
				//	.groupByKey(Grouped.with(new JsonSerde<>(String.class), new JsonSerde<>(String.class)))
					//.groupByKey(Grouped.with(null, userSerde))

				//	.windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
				//	.count(Materialized.as("product-counts"))
				//	.toStream()
					//.to("", Produced.with(Serdes.String(), Serdes.String()))

					.branch(new Predicate[]{isTea, isCoffee});

//			Predicate<Object, WordCount> isEnglish = (k, v) -> v.word.equals("english");
//			Predicate<Object, WordCount> isFrench =  (k, v) -> v.word.equals("french");
//			Predicate<Object, WordCount> isSpanish = (k, v) -> v.word.equals("spanish");

//			Cannot resolve method 'branch(org.apache.kafka.streams.kstream.Predicate<java.lang.String,com.wave.kafka.model.User>,
//			org.apache.kafka.streams.kstream.Predicate<java.lang.String,com.wave.kafka.model.User>)'
//			return input -> input
//					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
//					.groupBy((key, value) -> value)
//					.windowedBy(TimeWindows.of(Duration.ofSeconds(6)))
//					.count(Materialized.as("WordCounts-1"))
//					.toStream()
//					.map((key, value) -> new KeyValue<>(null,
//							new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))))
//					.branch(isEnglish, isFrench, isSpanish);
//

		}
	}

}
