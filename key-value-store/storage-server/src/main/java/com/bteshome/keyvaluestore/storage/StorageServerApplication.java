package com.bteshome.keyvaluestore.storage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(scanBasePackages = {"com.bteshome.keyvaluestore.storage", "com.bteshome.keyvaluestore.common"})
public class StorageServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(StorageServerApplication.class, args);
        /*ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(StorageServerApplication.class);
        context.start();
        context.close();*/
    }
}