package com.bteshome.onlinestore.admindashboard.dto;

import lombok.*;

import java.math.BigDecimal;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class Order {
    private String orderNumber;
    private String orderDatetime;
    private String username;
    private String firstName;
    private String lastName;
    private String email;
    private String status;
    private List<LineItem> lineItems;

    public double getTotalPrice() {
        return lineItems.stream()
                .mapToDouble(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())).doubleValue())
                .sum();
    }
}