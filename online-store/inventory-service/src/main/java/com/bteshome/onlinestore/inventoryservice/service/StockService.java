package com.bteshome.onlinestore.inventoryservice.service;

import com.bteshome.keyvaluestore.client.writers.ItemWriter;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.Tuple;
import com.bteshome.keyvaluestore.common.Tuple3;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

            List<Tuple3<Product, LogPosition, Integer>> products = new ArrayList<>(stockRequests.size());

            // validation stage
            for (StockRequest stockRequest : stockRequests) {
                Tuple<Product, LogPosition> productWithVersion = productRepository.getVersioned(stockRequest.getSkuCode());
                Product product = productWithVersion.first();
                LogPosition version = productWithVersion.second();
                if (product.getStockLevel() < stockRequest.getQuantity()) {
                    String errorMessage = "Product with skuCode '%s' is not in stock for the requested quantity '%s'."
                            .formatted(stockRequest.getSkuCode(), stockRequest.getQuantity());
                    log.error(errorMessage);
                    return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(errorMessage);
                }
                products.add(new Tuple3<>(product, version, stockRequest.getQuantity()));
            }

            List<Tuple3<Product, LogPosition, Integer>> succeededProducts = new ArrayList<>(products.size());
            Exception updateException = null;

            // update stage
            try {
                for (Tuple3<Product, LogPosition, Integer> productWithVersion : products) {
                    Product product = productWithVersion.first();
                    LogPosition version = productWithVersion.second();
                    int requestedQuantity = productWithVersion.third();
                    product.setStockLevel(product.getStockLevel() - requestedQuantity);
                    productRepository.put(product, version);
                    succeededProducts.add(productWithVersion);
                }
                log.debug("Successfully reserved {} stock items.", products.size());
                return ResponseEntity.ok().build();
            } catch (Exception e) {
                log.error("Failed to reserve stock items. Only {} of {} items succeeded: {}.",
                        succeededProducts.size(),
                        products.size(),
                        e.getMessage(),
                        e);
                updateException = e;
            }

            // rollback stage
            try {
                for (Tuple3<Product, LogPosition, Integer> productWithVersion : succeededProducts) {
                    Tuple<Product, LogPosition> productWithVersion2 = productRepository.getVersioned(productWithVersion.first().getSkuCode());
                    Product product = productWithVersion2.first();
                    LogPosition version = productWithVersion2.second();
                    int requestedQuantity = productWithVersion.third();
                    product.setStockLevel(product.getStockLevel() + productWithVersion.third());
                    productRepository.put(product, version);
                }
                log.debug("Successfully rolled back changes to reserve {} stock items.", succeededProducts.size());
                String errorMessage = "Failed to reserve stock items: %s.".formatted(updateException.getMessage());
                return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(errorMessage);
            } catch (Exception rollbackException) {
                // rollback failed
                String errorMessage1 = "Failed to reserve stock items: %s".formatted(updateException.getMessage());
                String errorMessage2 = "Failed to rollback changes made to reserve stock items: %s".formatted(rollbackException.getMessage());
                log.error(errorMessage2, rollbackException);
                return ResponseEntity.internalServerError().body("%s %s".formatted(errorMessage1, errorMessage2));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }
}