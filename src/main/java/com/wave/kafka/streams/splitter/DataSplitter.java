package com.wave.kafka.streams.splitter;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.wave.kafka.model.Preference;
import com.wave.kafka.model.User;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


@Configuration
public class DataSplitter {

    @Bean
    @SuppressWarnings("unchecked")
    public Function<KStream<String, String>, KStream<?, WordCount>[]> process() {



        Predicate<String, String> isTea = (k, v) -> v.contains(Preference.TEA.name());
        Predicate<String, String> isCoffee = (k, v) -> v.contains(Preference.COFFEE.name());

        System.out.println("User Splitterby tea/coffee");


			Predicate<Object, WordCount> isEnglish = (k, v) -> v.word.equalsIgnoreCase("tea");
			Predicate<Object, WordCount> isFrench =  (k, v) -> v.word.equalsIgnoreCase("coffee");
			//Predicate<Object, WordCount> isSpanish = (k, v) -> v.word.equals("spanish");

			return input -> input
					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.groupBy((key, value) -> value)
					.windowedBy(TimeWindows.of(Duration.ofSeconds(6)))
					.count(Materialized.as("WordCounts-1"))
					.toStream()
					.map((key, value) -> new KeyValue<>(null,
							new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))))
					.branch(isEnglish, isFrench);


    }

    static class WordCount {

        private String word;

        private long count;

        private Date start;

        private Date end;

        WordCount(String word, long count, Date start, Date end) {
            this.word = word;
            this.count = count;
            this.start = start;
            this.end = end;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public Date getStart() {
            return start;
        }

        public void setStart(Date start) {
            this.start = start;
        }

        public Date getEnd() {
            return end;
        }

        public void setEnd(Date end) {
            this.end = end;
        }
    }
}