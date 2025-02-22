package com.bteshome.onlinestore.orderservice.dto;

import com.bteshome.onlinestore.orderservice.model.NotificationStatus;
import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class OrderCreateResponse {
    private int httpStatus;
    private String errorMessage;
    private String infoMessage;
}