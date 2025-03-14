package com.bteshome.onlinestore.orderservice.dto;

import lombok.*;

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