package com.bteshome.onlinestore.inventoryservice.controller;

import com.bteshome.onlinestore.inventoryservice.dto.ProductRequest;
import com.bteshome.onlinestore.inventoryservice.service.ProductService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
@Slf4j
public class ProductController {
    @Autowired
    private ProductService productService;

    @PostMapping("/")
    public ResponseEntity<?> create(@RequestBody ProductRequest productRequest) {
        return productService.put(productRequest);
    }

    @GetMapping("/")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<?> getAll() {
        return productService.getAll();
    }
}