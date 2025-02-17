package com.bteshome.onlinestore.orderservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.bteshome.onlinestore.orderservice", "com.bteshome.keyvaluestore.common", "com.bteshome.keyvaluestore.client"})
public class OrderApplication {
	public static void main(String[] args) {
		SpringApplication.run(OrderApplication.class, args);
	}
}