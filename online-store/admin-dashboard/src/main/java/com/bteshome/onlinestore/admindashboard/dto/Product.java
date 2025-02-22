package com.bteshome.onlinestore.admindashboard.dto;

import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Product {
    private String name;
    private String description;
    private BigDecimal price;
    private String skuCode;
    private int stockLevel;
}
