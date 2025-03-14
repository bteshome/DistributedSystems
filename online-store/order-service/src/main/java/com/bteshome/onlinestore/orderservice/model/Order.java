package com.bteshome.onlinestore.orderservice.model;

import lombok.*;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class Order implements Serializable {
    private String orderNumber;
    private long orderTimestamp;
    private String username;
    private String firstName;
    private String lastName;
    private String email;
    private OrderStatus status;
    private List<LineItem> lineItems;
}