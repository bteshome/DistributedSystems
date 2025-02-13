package com.bteshome.onlinestore.orderservice.client;

import com.bteshome.onlinestore.orderservice.config.AppSettings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.util.List;

@Slf4j
public class InventoryClient {
    @Autowired
    private AppSettings appSettings;

    @Autowired
    RestClient.Builder restClientBuilder;

    public void reserveStockItems(List<InventoryRequest> items) {
        restClientBuilder.build()
                .put()
                .uri("%s/api/stock/".formatted(appSettings.getInventoryServiceUrl()))
                .contentType(MediaType.APPLICATION_JSON)
                .body(items)
                .retrieve()
                .toEntity(String.class);
    }
}