package com.bteshome.onlinestore.orderservice.controller;

import com.bteshome.onlinestore.orderservice.dto.OrderCreateResponse;
import com.bteshome.onlinestore.orderservice.dto.OrderRequest;
import com.bteshome.onlinestore.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {
    @Autowired
    private OrderService orderService;

    @PostMapping("/create/")
    public ResponseEntity<OrderCreateResponse> create(@RequestBody OrderRequest orderRequest) {
        return orderService.create(orderRequest);
    }

    @GetMapping("/list/")
    public ResponseEntity<?> list() {
        return orderService.getAll();
    }

    @GetMapping("/query/")
    public ResponseEntity<?> queryByEmail(@RequestParam("email") String email) {
        return orderService.queryByEmail(email);
    }
}