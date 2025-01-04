package com.bteshome.onlinestore.inventoryservice.controller;

import com.bteshome.onlinestore.inventoryservice.InventoryException;
import com.bteshome.onlinestore.inventoryservice.dto.StockRequest;
import com.bteshome.onlinestore.inventoryservice.service.StockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/stock")
@RequiredArgsConstructor
@Slf4j
public class InventoryController {
    @Autowired
    private StockService stockService;

    @GetMapping("/{sku-code}/")
    @ResponseStatus(HttpStatus.OK)
    public boolean isInStock(@PathVariable("sku-code") String skuCode, @RequestParam int quantity) {
        return stockService.isInStock(skuCode, quantity);
    }

    @PutMapping("/")
    public ResponseEntity<String> reserveStockItems(@RequestBody List<StockRequest> stockRequests) {
        try {
            stockService.reserveStockItems(stockRequests);
            return ResponseEntity.ok().build();
        } catch (InventoryException e) {
            log.error(e.getMessage());
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
}
