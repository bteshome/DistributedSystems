package com.bteshome.onlinestore.orderservice.service;

import com.bteshome.onlinestore.orderservice.client.InventoryClient;
import com.bteshome.onlinestore.orderservice.client.InventoryRequest;
import com.bteshome.onlinestore.orderservice.config.AppSettings;
import com.bteshome.onlinestore.orderservice.dto.*;
import com.bteshome.onlinestore.orderservice.model.LineItem;
import com.bteshome.onlinestore.orderservice.model.Order;
import com.bteshome.onlinestore.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class OrderService {
    @Autowired
    private final OrderRepository orderRepository;
    @Autowired
    private final InventoryClient inventoryClient;
    @Autowired
    private final StreamBridge streamBridge;
    @Autowired
    private final AppSettings appSettings;

    public void create(OrderRequest orderRequest) {
        log.debug("Creating an order...");

        Order order = mapToOrder(orderRequest);

        var addInventoryQuantitiesRequest = order.getLineItems().stream().map(item ->
                    InventoryRequest.builder()
                            .skuCode(item.getSkuCode())
                            .quantity(-item.getQuantity())
                            .build())
                .toList();

        inventoryClient.reserveStockItems(addInventoryQuantitiesRequest);

        orderRepository.save(order);

        createOrderCreatedEvent(order);

        log.debug("Order created successfully");
    }

    private void createOrderCreatedEvent(Order order) {
        if (appSettings.isNotificationDisabled()) {
            log.debug("Notification is disabled. Skipping OrderCreatedEvent event creation for order: {}", order.getOrderNumber());
            return;
        }

        var orderCreatedEvent = new OrderCreatedEvent(order.getOrderNumber(), order.getEmail());
        streamBridge.send("orderCreated-out-0", orderCreatedEvent);

        log.debug("Created OrderCreatedEvent for order: {}", order.getOrderNumber());
    }

    public List<OrderResponse> getAll() {
        Iterable<Order> orders = orderRepository.findAll();
        return StreamSupport.stream(orders.spliterator(), false)
                .map(this::mapToOrderResponse)
                .toList();
    }

    private Order mapToOrder(OrderRequest orderRequest) {
        var order = Order.builder()
                .orderNumber(UUID.randomUUID().toString())
                .email(orderRequest.getEmail())
                .notificationStatus("PENDING")
                .build();
        var lineItems = orderRequest.getLineItems().stream().map(i -> {
            var item = mapToLineItem(i);
            item.setOrder(order);
            return item;
        }).toList();
        order.setLineItems(lineItems);
        return order;
    }

    private LineItem mapToLineItem(LineItemRequest lineItemRequest) {
        return LineItem.builder()
                .skuCode(lineItemRequest.getSkuCode())
                .quantity(lineItemRequest.getQuantity())
                .price(lineItemRequest.getPrice())
                .build();
    }

    private OrderResponse mapToOrderResponse(Order order) {
        var lineItems = order.getLineItems().stream().map(this::mapToLineItemResponse).toList();

        return OrderResponse.builder()
                .id(order.getId())
                .orderNumber(order.getOrderNumber())
                .email(order.getEmail())
                .notificationStatus(order.getNotificationStatus())
                .lineItems(lineItems)
                .build();
    }

    private LineItemResponse mapToLineItemResponse(LineItem lineItem) {
        return LineItemResponse.builder()
                .id(lineItem.getId())
                .skuCode(lineItem.getSkuCode())
                .quantity(lineItem.getQuantity())
                .price(lineItem.getPrice())
                .build();
    }
}