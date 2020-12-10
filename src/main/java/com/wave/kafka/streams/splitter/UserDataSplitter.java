package com.wave.kafka.streams.splitter;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.wave.kafka.model.Preference;
import com.wave.kafka.model.User;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.HashMap;
import java.util.Map;

import static com.wave.kafka.streams.splitter.UserProcessorBinding.*;
import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME;



public class UserDataSplitter {

    ObjectMapper mapper = new ObjectMapper();
    Serde<User> userJsonSerde = new JsonSerde<>(User.class, mapper);

    @Bean(DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public Map<String, Object> userStreamsConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-user");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, userJsonSerde.getClass());
        return config;
    }

    Predicate<String, User> isTea = (k, v) -> v.getPreference().equals(Preference.TEA);
    Predicate<String, User> isCoffee = (k, v) -> v.getPreference().equals(Preference.COFFEE);

    @StreamListener//(INPUTUSERSTREAM)
    @SendTo({OUTPUTUSERSTREAMTEA, OUTPUTUSERSTREAMCOFFEE})
    public KStream<String, User>[] handle1(@Input(INPUTUSERSTREAM) KStream<String, User> kSink) {

        System.out.println("In the UppercaseSink SINK  handle1 kSink");
        kSink.print(Printed.toSysOut());

        return kSink.branch(isTea, isCoffee);

    }

    @Bean(name = "customStreamBuilder")
    public FactoryBean<StreamsBuilder> customStreamBuilder(
            @Qualifier(DEFAULT_STREAMS_CONFIG_BEAN_NAME)
                    ObjectProvider<KafkaStreamsConfiguration> streamsConfigProvider,
            ObjectProvider<StreamsBuilderFactoryBeanCustomizer> customizerProvider) {
        Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-custom");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, userJsonSerde.getClass());


        KafkaStreamsConfiguration streamsConfig = new KafkaStreamsConfiguration(config);
        if (streamsConfig != null) {
            //streamsConfig.asProperties().setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.serdeFrom(new UserSerialiser(), new UserDeserializer()).getClass()));
            StreamsBuilderFactoryBean fb = new StreamsBuilderFactoryBean(streamsConfig);
            StreamsBuilderFactoryBeanCustomizer customizer = customizerProvider.getIfUnique();
            if (customizer != null) {
                customizer.configure(fb);
            }



            return fb;
        }
        else {
            throw new UnsatisfiedDependencyException("customStreamBuilder",
                    "customStreamBuilder", "streamsConfig", "There is no '" +
                    DEFAULT_STREAMS_CONFIG_BEAN_NAME + "' " + "customStreamBuilder" +
                    " bean in the application context.\n" +
                    "Consider declaring one or don't use @EnableKafkaStreams.");
        }
    }

    @Bean//("userstream")
    public KStream<String, User> userstream(@Qualifier("customStreamBuilder") StreamsBuilder kStreamBuilder) {

        System.out.println("in the sink 333:: ");
        KStream<String, User> stream = kStreamBuilder.stream("inputuserstream");


        System.out.println("in the user Sink 333 :: ");
        //  stream.print(Printed.toSysOut());


        stream.mapValues(v -> {
            System.out.println("IN the Sink Stream 333:: " + v);
            return v;
        }).to(INPUTUSERSTREAM);
        return stream;
    }

    public static void main(String[] args) {
        SpringApplication.run(UserDataSplitter.class, args);
    }

}