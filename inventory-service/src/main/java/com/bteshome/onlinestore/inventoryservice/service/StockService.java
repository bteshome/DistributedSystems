package com.bteshome.onlinestore.inventoryservice.service;

import com.bteshome.keyvaluestore.client.writers.ItemWriter;
import com.bteshome.onlinestore.inventoryservice.InventoryException;
import com.bteshome.onlinestore.inventoryservice.dto.StockRequest;
import com.bteshome.onlinestore.inventoryservice.model.Product;
import com.bteshome.onlinestore.inventoryservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class StockService {
    @Autowired
    private ProductRepository productRepository;

    private ResponseEntity<?> find(String skuCode) {
        try {
            Product product = productRepository.get(skuCode);
            if (product == null)
                return ResponseEntity.notFound().build();
            return ResponseEntity.ok(product);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    public ResponseEntity<?> isInStock(String skuCode, int quantity) {
        try {
            Product product = productRepository.get(skuCode);
            if (product == null)
                return ResponseEntity.ok(false);
            return ResponseEntity.ok(product.getStockLevel() >= quantity);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    public ResponseEntity<?> reserveStockItems(List<StockRequest> stockRequests) {
        try {
            log.debug("Attempting to reserve stock items");

            for (StockRequest stockRequest : stockRequests) {
                Product product = productRepository.get(stockRequest.getSkuCode());
                int quantity = product.getStockLevel() - stockRequest.getQuantity();
                // TODO - work on compensating transactions in case of any error
                if (quantity < 0) {
                    String errorMessage = "Product with skuCode '%s' is not in stock for the requested quantity '%s'."
                            .formatted(stockRequest.getSkuCode(), stockRequest.getQuantity());
                    log.error(errorMessage);
                    return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(errorMessage);
                }
                product.setStockLevel(quantity);
                // TODO - in the key value store system, consider optimistic concurrency
                productRepository.put(product);
            }

            log.debug("Successfully reserved stock items");

            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }
}