package com.bteshome.onlinestore.orderservice.repository;

import com.bteshome.onlinestore.orderservice.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {
    Order findFirstByOrderNumber(String orderNumber);
}
