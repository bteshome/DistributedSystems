package com.bteshome.onlinestore.inventoryservice.service;

import com.bteshome.onlinestore.inventoryservice.dto.ProductRequest;
import com.bteshome.onlinestore.inventoryservice.dto.ProductResponse;
import com.bteshome.onlinestore.inventoryservice.model.Product;
import com.bteshome.onlinestore.inventoryservice.repository.InventoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.StreamSupport;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class ProductService {
    @Autowired
    private final InventoryRepository productRepository;

    public void create(ProductRequest productRequest) {
        log.debug("Creating product: {}", productRequest.getName());
        Product product = mapToProduct(productRequest);
        product = productRepository.save(product);
        log.debug("Successfully created product: {}", productRequest.getName());
    }

    public List<ProductResponse> getAll() {
        Iterable<Product> products = productRepository.findAll();
        return StreamSupport.stream(products.spliterator(), false)
                .map(this::mapToProductResponse)
                .toList();
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
                .id(product.getId())
                .name(product.getName())
                .description(product.getDescription())
                .price(product.getPrice())
                .skuCode(product.getSkuCode())
                .stockLevel(product.getStockLevel())
                .build();
    }
}