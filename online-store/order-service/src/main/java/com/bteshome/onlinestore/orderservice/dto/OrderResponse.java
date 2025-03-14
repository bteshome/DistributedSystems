package com.bteshome.onlinestore.orderservice.dto;

import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class OrderResponse {
    private String orderNumber;
    private String orderDatetime;
    private String username;
    private String firstName;
    private String lastName;
    private String email;
    private String status;
    private List<LineItemResponse> lineItems;
}