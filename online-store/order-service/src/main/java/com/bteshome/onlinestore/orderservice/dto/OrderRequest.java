package com.bteshome.onlinestore.orderservice.dto;

import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class OrderRequest {
    private String email;
    private List<LineItemRequest> lineItems;
}
