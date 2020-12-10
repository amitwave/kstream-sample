package com.wave.kafka.model;

import lombok.Data;
import lombok.Getter;

@Data
@Getter
public class User {

    private Integer id;
    private String name;
    private Long dob;
    private Preference preference;



}
