package com.bteshome.keyvaluestore.admindashboard.dto;

import com.bteshome.keyvaluestore.client.requests.AckType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemPutBatchDto {
    private String table;
    private int numItems;
    private AckType ack;
}