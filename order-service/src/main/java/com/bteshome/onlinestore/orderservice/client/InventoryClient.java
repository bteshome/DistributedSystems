package com.bteshome.onlinestore.orderservice.client;

import com.bteshome.onlinestore.orderservice.InventoryClientException;
import com.bteshome.onlinestore.orderservice.config.AppSettings;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
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

    @CircuitBreaker(name = "inventory_client", fallbackMethod = "reserveStockItemsFallback")
    public void reserveStockItems(List<InventoryRequest> items) {
        restClientBuilder.build()
                .put()
                .uri("%s/api/stock/".formatted(appSettings.getInventoryServiceUrl()))
                .contentType(MediaType.APPLICATION_JSON)
                .body(items)
                .retrieve()
                .toBodilessEntity();
    }

    public void reserveStockItemsFallback(List<InventoryRequest> items, Throwable throwable) {
        throw new InventoryClientException("Failed to reserve inventory stock items. Inventory service unavailable.", throwable);
    }
}