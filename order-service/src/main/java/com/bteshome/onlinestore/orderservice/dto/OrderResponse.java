package com.bteshome.onlinestore.orderservice.dto;

import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class OrderResponse {
    private long id;
    private String orderNumber;
    private String email;
    private String notificationStatus;
    private List<LineItemResponse> lineItems;
}