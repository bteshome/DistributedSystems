package com.bteshome.keyvaluestore.admindashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.bteshome.keyvaluestore.admindashboard", "com.bteshome.keyvaluestore.common", "com.bteshome.keyvaluestore.client"})
public class AdminDashboardApplication {
    public static void main(String[] args) {
        SpringApplication.run(AdminDashboardApplication.class, args);
    }
}