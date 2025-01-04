package com.bteshome.onlinestore.orderservice.functions;

import com.bteshome.onlinestore.orderservice.OrderException;
import com.bteshome.onlinestore.orderservice.dto.OrderCreatedNotificationSentEvent;
import com.bteshome.onlinestore.orderservice.model.Order;
import com.bteshome.onlinestore.orderservice.repository.OrderRepository;
import lombok.extern.slf4j.Slf4j;
import java.util.function.Consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(value = "notification-disabled", havingValue = "false", matchIfMissing = true)
@Slf4j
public class Functions {
    @Autowired
    private OrderRepository orderRepository;

    @Bean
    public Consumer<OrderCreatedNotificationSentEvent> orderCreatedNotificationSent() {
        return notificationSentEvent -> {
            Order order = orderRepository.findFirstByOrderNumber(notificationSentEvent.orderNumber());
            if (order == null) {
                log.error("Order '{}' not found. Unable to update notification status.", notificationSentEvent.orderNumber());
            } else {
                order.setNotificationStatus(notificationSentEvent.status());
                orderRepository.save(order);
                log.debug("Updated notification status for order: {}", notificationSentEvent);
            }
        };
    }
}
