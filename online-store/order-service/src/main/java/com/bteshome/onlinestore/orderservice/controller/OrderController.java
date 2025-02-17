package com.bteshome.onlinestore.orderservice.controller;

import com.bteshome.onlinestore.orderservice.dto.OrderRequest;
import com.bteshome.onlinestore.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {
    @Autowired
    private OrderService orderService;

    @PostMapping("/")
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<?> create(@RequestBody OrderRequest orderRequest) {
        return orderService.create(orderRequest);
    }

    @GetMapping("/")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<?> getAll() {
        return orderService.getAll();
    }
}