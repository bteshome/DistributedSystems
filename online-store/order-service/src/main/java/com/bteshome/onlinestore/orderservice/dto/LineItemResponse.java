package com.bteshome.onlinestore.orderservice.dto;

import lombok.*;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class LineItemResponse {
    private String skuCode;
    private int quantity;
    private BigDecimal price;
}