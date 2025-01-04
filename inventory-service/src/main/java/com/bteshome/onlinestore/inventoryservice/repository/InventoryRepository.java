package com.bteshome.onlinestore.inventoryservice.repository;

import com.bteshome.onlinestore.inventoryservice.model.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface InventoryRepository extends JpaRepository<Product, Long> {
    Product findFirstBySkuCode(String skuCode);
}