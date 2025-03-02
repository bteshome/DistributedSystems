package com.bteshome.onlinestore.orderservice.service;

import com.bteshome.onlinestore.orderservice.model.NotificationStatus;
import com.bteshome.onlinestore.orderservice.model.Order;
import com.bteshome.onlinestore.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {
    @Autowired
    private final OrderRepository orderRepository;

    public void send(Order order) {
        try {
            // TODO - send notification to customer
            order.setNotificationStatus(NotificationStatus.SENT);
            orderRepository.put(order);
            log.debug("Updated notification status for order: {}", order.getOrderNumber());
        } catch (Exception e) {
            log.error("Failed to update notification status for order: {}.", order.getOrderNumber(), e);
        }
    }
}