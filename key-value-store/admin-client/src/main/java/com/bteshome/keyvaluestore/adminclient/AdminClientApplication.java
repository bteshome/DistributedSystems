package com.bteshome.keyvaluestore.adminclient;

import com.bteshome.keyvaluestore.common.MetadataRefresher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.bteshome.keyvaluestore.adminclient", "com.bteshome.keyvaluestore.common"})
public class AdminClientApplication implements CommandLineRunner {
    @Autowired
    MetadataRefresher metadataRefresher;

    public static void main(String[] args) {
        SpringApplication.run(AdminClientApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        //metadataRefresher.schedule();
        metadataRefresher.fetch();
    }
}