package com.bteshome.onlinestore.inventoryservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.bteshome.onlinestore.inventoryservice", "com.bteshome.keyvaluestore.common", "com.bteshome.keyvaluestore.client"})
public class InventoryApplication {
	public static void main(String[] args) {
		SpringApplication.run(InventoryApplication.class, args);
	}
}