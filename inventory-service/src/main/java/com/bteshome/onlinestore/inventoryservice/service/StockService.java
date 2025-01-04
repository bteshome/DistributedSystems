package com.bteshome.onlinestore.inventoryservice.service;

import com.bteshome.onlinestore.inventoryservice.InventoryException;
import com.bteshome.onlinestore.inventoryservice.dto.StockRequest;
import com.bteshome.onlinestore.inventoryservice.model.Product;
import com.bteshome.onlinestore.inventoryservice.repository.InventoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class StockService {
    @Autowired
    private final InventoryRepository inventoryRepository;

    private Product find(String skuCode) {
        Product product = inventoryRepository.findFirstBySkuCode(skuCode);
        if (product == null) {
            throw new InventoryException("Product with sku code '%s' does not exist.".formatted(skuCode));
        }
        return product;
    }

    public boolean isInStock(String skuCode, int quantity) {
        return find(skuCode).getStockLevel() >= quantity;
    }

    public void reserveStockItems(List<StockRequest> stockRequests) {
        log.debug("Attempting to reserve stock items");

        for (StockRequest stockRequest : stockRequests) {
            Product product = find(stockRequest.getSkuCode());
            int quantity = product.getStockLevel() - stockRequest.getQuantity();
            if (quantity < 0) {
                throw new InventoryException("Product with skuCode '%s' is not in stock for the requested quantity '%s'."
                        .formatted(stockRequest.getSkuCode(), stockRequest.getQuantity()));
            }
            product.setStockLevel(quantity);
            inventoryRepository.save(product);
        }

        log.debug("Successfully reserved stock items");
    }
}