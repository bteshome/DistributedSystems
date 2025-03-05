package com.bteshome.apigateway.ratelimiter;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Rule {
    private UUID id;
    private String api;
    private boolean isPerClient;
    private String granularity;
    private int threshold;
}