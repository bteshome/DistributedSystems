package com.bteshome.onlinestore.orderservice.dto;

import com.bteshome.onlinestore.orderservice.model.NotificationStatus;

public record OrderCreatedNotificationSentEvent(String orderNumber, NotificationStatus status){}

