package com.bteshome.onlinestore.inventoryservice.dto;

import lombok.*;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class ProductResponse {
    private long id;
    private String name;
    private String description;
    private BigDecimal price;
    private String skuCode;
    private int stockLevel;
}
