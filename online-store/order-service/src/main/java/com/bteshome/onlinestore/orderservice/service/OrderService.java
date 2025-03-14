package com.bteshome.onlinestore.orderservice.service;

import com.bteshome.onlinestore.orderservice.OrderException;
import com.bteshome.onlinestore.orderservice.client.InventoryClient;
import com.bteshome.onlinestore.orderservice.client.InventoryRequest;
import com.bteshome.onlinestore.orderservice.config.AppSettings;
import com.bteshome.onlinestore.orderservice.dto.*;
import com.bteshome.onlinestore.orderservice.model.LineItem;
import com.bteshome.onlinestore.orderservice.model.OrderStatus;
import com.bteshome.onlinestore.orderservice.model.Order;
import com.bteshome.onlinestore.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderService {
    @Autowired
    private final OrderRepository orderRepository;
    @Autowired
    private final InventoryClient inventoryClient;
    @Autowired
    private final AppSettings appSettings;
    @Autowired
    private final NotificationService notificationService;

    public ResponseEntity<OrderCreateResponse> create(OrderRequest orderRequest) {
        try {
            validateOrderRequest(orderRequest);

            Order order = mapToOrder(orderRequest);

            log.debug("Creating order {}.", order.getOrderNumber());

            List<InventoryRequest> addInventoryQuantitiesRequest = order.getLineItems()
                    .stream()
                    .collect(Collectors.groupingBy(LineItem::getSkuCode, Collectors.counting()))
                    .entrySet()
                    .stream()
                    .filter(item -> item.getValue() > 0)
                    .map(item -> InventoryRequest.builder()
                            .skuCode(item.getKey())
                            .quantity(item.getValue().intValue())
                            .build())
                    .toList();

            inventoryClient.reserveStockItems(addInventoryQuantitiesRequest);
            orderRepository.put(order);
            createOrderCreatedEvent(order);

            log.debug("Order {} created successfully.", order.getOrderNumber());

            return ResponseEntity.status(HttpStatus.OK).body(
                    OrderCreateResponse.builder()
                            .httpStatus(HttpStatus.OK.value())
                            .infoMessage("Order placed successfully.")
                            .build());
        } catch (OrderException e) {
            return ResponseEntity.status(HttpStatus.OK).body(
                    OrderCreateResponse.builder()
                            .httpStatus(HttpStatus.BAD_REQUEST.value())
                            .errorMessage(e.getMessage())
                            .build());
        } catch (HttpClientErrorException e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.OK).body(
                    OrderCreateResponse.builder()
                            .httpStatus(HttpStatus.UNPROCESSABLE_ENTITY.value())
                            .errorMessage(e.getMessage())
                            .build());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.OK).body(
                    OrderCreateResponse.builder()
                            .httpStatus(HttpStatus.INTERNAL_SERVER_ERROR.value())
                            .errorMessage(e.getMessage())
                            .build());
        }
    }

    public ResponseEntity<?> queryByUsername(String username) {
        try {
            List<OrderResponse> orders = orderRepository.queryByUsername(username).map(this::mapToOrderResponse).toList();
            return ResponseEntity.ok(orders);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    public ResponseEntity<?> getAll() {
        try {
            List<OrderResponse> orders = orderRepository.getAll().map(this::mapToOrderResponse).toList();
            return ResponseEntity.ok(orders);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    private void validateOrderRequest(OrderRequest orderRequest) {
        if (Strings.isBlank(orderRequest.getEmail()))
            throw new OrderException("Email is required.");

        if (Strings.isBlank(orderRequest.getUsername()))
            throw new OrderException("Username is required.");

        if (Strings.isBlank(orderRequest.getFirstName()))
            throw new OrderException("First Name is required.");

        if (Strings.isBlank(orderRequest.getLastName()))
            throw new OrderException("Last Name is required.");

        if (orderRequest.getLineItems() == null || orderRequest.getLineItems().isEmpty())
            throw new OrderException("At least one line item is required.");
    }

    private void createOrderCreatedEvent(Order order) {
        if (appSettings.isNotificationDisabled()) {
            log.debug("Notification is disabled. Skipping OrderCreatedEvent event creation for order: {}", order.getOrderNumber());
            return;
        }

        CompletableFuture.runAsync(() -> notificationService.send(order));

        log.debug("Created OrderCreatedEvent for order: {}", order.getOrderNumber());
    }

    private Order mapToOrder(OrderRequest orderRequest) {
        var order = Order.builder()
                .orderNumber(UUID.randomUUID().toString())
                .orderTimestamp(System.currentTimeMillis())
                .username(orderRequest.getUsername())
                .firstName(orderRequest.getFirstName())
                .lastName(orderRequest.getLastName())
                .email(orderRequest.getEmail())
                .status(OrderStatus.PENDING)
                .build();
        var lineItems = orderRequest.getLineItems().stream().map(this::mapToLineItem).toList();
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
        List<LineItemResponse> lineItems = order.getLineItems().stream().map(this::mapToLineItemResponse).toList();
        String orderDatetime = Instant.ofEpochMilli(order.getOrderTimestamp())
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("MM/dd/yy h:mm a"));

        return OrderResponse.builder()
                .orderNumber(order.getOrderNumber())
                .orderDatetime(orderDatetime)
                .username(order.getUsername())
                .firstName(order.getFirstName())
                .lastName(order.getLastName())
                .email(order.getEmail())
                .status(order.getStatus().toString())
                .lineItems(lineItems)
                .build();
    }

    private LineItemResponse mapToLineItemResponse(LineItem lineItem) {
        return LineItemResponse.builder()
                .skuCode(lineItem.getSkuCode())
                .quantity(lineItem.getQuantity())
                .price(lineItem.getPrice())
                .build();
    }
}