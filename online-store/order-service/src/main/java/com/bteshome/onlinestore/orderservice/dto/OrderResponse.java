package com.bteshome.onlinestore.orderservice.dto;

import com.bteshome.onlinestore.orderservice.model.NotificationStatus;
import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class OrderResponse {
    private String orderNumber;
    private String email;
    private NotificationStatus notificationStatus;
    private List<LineItemResponse> lineItems;
}