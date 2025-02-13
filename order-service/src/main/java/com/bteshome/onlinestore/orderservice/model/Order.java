package com.bteshome.onlinestore.orderservice.model;

import lombok.*;

import java.io.Serializable;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class Order implements Serializable {
    private String orderNumber;
    private String email;
    private NotificationStatus notificationStatus;
    private List<LineItem> lineItems;
}