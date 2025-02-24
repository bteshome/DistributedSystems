package com.bteshome.keyvaluestore.admindashboard.dto;

import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemPutDto {
    private String table;
    private String key;
    private String value;
    private AckType ack;
    private int maxRetries;
    private String indexKeys;
}