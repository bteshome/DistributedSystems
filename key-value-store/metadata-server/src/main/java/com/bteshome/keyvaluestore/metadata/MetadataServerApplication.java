package com.bteshome.keyvaluestore.metadata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.bteshome.keyvaluestore.metadata", "com.bteshome.keyvaluestore.common"})
public class MetadataServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MetadataServerApplication.class, args);
    }
}