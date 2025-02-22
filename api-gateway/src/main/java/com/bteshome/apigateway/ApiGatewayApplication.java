package com.bteshome.apigateway;

import com.bteshome.apigateway.common.AppSettings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.bteshome.apigateway", "com.bteshome.keyvaluestore.common", "com.bteshome.keyvaluestore.client"})
public class ApiGatewayApplication implements CommandLineRunner {
	@Autowired
	AppSettings appSettings;

	public static void main(String[] args) {
		SpringApplication.run(ApiGatewayApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		appSettings.print();
	}
}
