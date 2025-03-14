package com.bteshome.onlinestore.orderservice.dto;

import com.bteshome.onlinestore.orderservice.model.OrderStatus;

public record OrderCreatedNotificationSentEvent(String orderNumber, OrderStatus status){}

