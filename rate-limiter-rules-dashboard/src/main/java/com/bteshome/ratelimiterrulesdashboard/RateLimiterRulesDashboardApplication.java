package com.bteshome.ratelimiterrulesdashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.bteshome.ratelimiterrulesdashboard", "com.bteshome.keyvaluestore.common", "com.bteshome.keyvaluestore.client"})
public class RateLimiterRulesDashboardApplication {
    public static void main(String[] args) {
        SpringApplication.run(RateLimiterRulesDashboardApplication.class, args);
    }
}