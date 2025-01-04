package com.bteshome.onlinestore.inventoryservice.controller;

import com.bteshome.onlinestore.inventoryservice.dto.ProductRequest;
import com.bteshome.onlinestore.inventoryservice.dto.ProductResponse;
import com.bteshome.onlinestore.inventoryservice.service.ProductService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
@Slf4j
public class ProductController {
    @Autowired
    private ProductService productService;

    @PostMapping("/")
    @ResponseStatus(HttpStatus.CREATED)
    public void create(
            @RequestBody ProductRequest productRequest) {
        productService.create(productRequest);
    }

    @GetMapping("/")
    @ResponseStatus(HttpStatus.OK)
    public List<ProductResponse> getAll() {
        return productService.getAll();
    }
}