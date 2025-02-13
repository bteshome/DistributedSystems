package com.bteshome.onlinestore.orderservice.model;

import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class LineItem implements Serializable {
    private String skuCode;
    private int quantity;
    private BigDecimal price;
}