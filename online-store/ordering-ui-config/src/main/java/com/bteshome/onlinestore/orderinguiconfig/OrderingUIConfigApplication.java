package com.bteshome.onlinestore.orderinguiconfig;

import com.bteshome.onlinestore.orderinguiconfig.common.AppSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderingUIConfigApplication implements CommandLineRunner {
	@Autowired
	AppSettings appSettings;

	public static void main(String[] args) {
		SpringApplication.run(OrderingUIConfigApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		appSettings.print();
	}
}