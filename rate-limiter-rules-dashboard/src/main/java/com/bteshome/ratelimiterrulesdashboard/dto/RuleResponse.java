package com.bteshome.ratelimiterrulesdashboard.dto;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RuleResponse {
    private long id;
    private String api;
    private boolean isPerClient;
    private String granularity;
    private int threshold;
}
