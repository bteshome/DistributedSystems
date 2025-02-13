package com.bteshome.onlinestore.inventoryservice.model;

import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class Product implements Serializable {
    private String name;
    private String description;
    private BigDecimal price;
    private String skuCode;
    private int stockLevel;
}