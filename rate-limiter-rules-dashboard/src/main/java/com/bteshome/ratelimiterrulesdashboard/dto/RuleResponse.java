package com.bteshome.ratelimiterrulesdashboard.dto;

import lombok.*;

import java.util.UUID;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RuleResponse {
    private UUID id;
    private String api;
    private boolean isPerClient;
    private String granularity;
    private int threshold;
}
