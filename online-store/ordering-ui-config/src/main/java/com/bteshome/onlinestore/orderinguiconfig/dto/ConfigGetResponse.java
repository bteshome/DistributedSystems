package com.bteshome.onlinestore.orderinguiconfig.dto;

import lombok.*;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class ConfigGetResponse {
    private int httpStatus;
    private String errorMessage;
    private String value;
}
