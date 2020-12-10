package com.wave.kafka.streams.splitter;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;


public interface UserProcessorBinding {

    String INPUTUSERSTREAM = "inputuserstream";

    String OUTPUTUSERSTREAMTEA = "outputuserstreamtea";
    String OUTPUTUSERSTREAMCOFFEE = "outputuserstreamcoffee";


    @Input(INPUTUSERSTREAM)
    KStream inputStream();


    @Output(OUTPUTUSERSTREAMTEA)
    KStream outputStreamtea();


    @Output(OUTPUTUSERSTREAMCOFFEE)
    KStream outputStreamcoffee();


}
