package com.bteshome.onlinestore.inventoryservice.service;

import com.bteshome.onlinestore.inventoryservice.dto.ProductRequest;
import com.bteshome.onlinestore.inventoryservice.dto.ProductResponse;
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
public class ProductService {
    @Autowired
    private ProductRepository productRepository;

    public ResponseEntity<?> put(ProductRequest productRequest) {
        try {
            log.debug("Creating or updating product: {}", productRequest.getName());

            Product product = mapToProduct(productRequest);
            productRepository.put(product);

            log.debug("Successfully created or updated product: {}", productRequest.getName());

            return ResponseEntity.status(HttpStatus.CREATED.value()).build();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    public ResponseEntity<?> getAll() {
        try {
            List<ProductResponse> products = productRepository.getAll().map(this::mapToProductResponse).toList();
            return ResponseEntity.ok(products);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    private Product mapToProduct(ProductRequest productRequest) {
        return Product.builder()
                .name(productRequest.getName())
                .description(productRequest.getDescription())
                .price(productRequest.getPrice())
                .skuCode(productRequest.getSkuCode())
                .stockLevel(productRequest.getStockLevel())
                .build();
    }

    private ProductResponse mapToProductResponse(Product product) {
        return ProductResponse.builder()
                .name(product.getName())
                .description(product.getDescription())
                .price(product.getPrice())
                .skuCode(product.getSkuCode())
                .stockLevel(product.getStockLevel())
                .build();
    }
}