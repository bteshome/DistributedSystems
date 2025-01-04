package com.bteshome.onlinestore.orderservice.controller;

import com.bteshome.onlinestore.orderservice.InventoryClientException;
import com.bteshome.onlinestore.orderservice.OrderException;
import com.bteshome.onlinestore.orderservice.dto.OrderRequest;
import com.bteshome.onlinestore.orderservice.dto.OrderResponse;
import com.bteshome.onlinestore.orderservice.service.OrderService;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {
    @Autowired
    private OrderService orderService;

    @PostMapping("/")
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<String> create(
            @RequestBody OrderRequest orderRequest) {
        try {
            orderService.create(orderRequest);
            return ResponseEntity.status(HttpStatus.CREATED).body("Order placed successfully.");
        } catch (InventoryClientException e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Service unavailable, please try again later.");
        } catch (OrderException e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.badRequest().body("An error occurred.");
        }
    }

    @GetMapping("/")
    @ResponseStatus(HttpStatus.OK)
    public List<OrderResponse> getAll() {
        return orderService.getAll();
    }
}