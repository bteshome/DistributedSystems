package com.bteshome.ratelimiterrulesdashboard.dto;

import jakarta.persistence.Column;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RuleRequest {
    private String api;
    private boolean isPerClient;
    private String granularity;
    private int threshold;
}
