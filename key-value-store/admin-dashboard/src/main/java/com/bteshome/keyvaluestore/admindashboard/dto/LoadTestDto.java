package com.bteshome.keyvaluestore.admindashboard.dto;

import com.bteshome.keyvaluestore.client.requests.AckType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Duration;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class LoadTestDto {
    private String table;
    private int requestsPerSecond;
    private Duration duration;
    private AckType ack;
    private int maxRetries;
}