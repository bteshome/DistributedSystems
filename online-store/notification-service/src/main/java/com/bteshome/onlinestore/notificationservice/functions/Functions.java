package com.bteshome.onlinestore.notificationservice.functions;

import com.bteshome.onlinestore.notificationservice.dto.OrderCreatedEvent;
import com.bteshome.onlinestore.notificationservice.dto.OrderCreatedNotificationSentEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Function;

@Configuration
@Slf4j
public class Functions {
    @Bean
    public Function<OrderCreatedEvent, OrderCreatedNotificationSentEvent> orderCreated() {
        return orderCreatedEvent -> {
            log.info("Sending email to: {}", orderCreatedEvent.email());
            // TODO - send email actually.
            return new OrderCreatedNotificationSentEvent(orderCreatedEvent.orderNumber(), "SENT");
        };
    }
}
