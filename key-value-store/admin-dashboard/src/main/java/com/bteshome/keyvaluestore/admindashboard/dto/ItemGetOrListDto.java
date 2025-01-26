package com.bteshome.keyvaluestore.admindashboard.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemGetOrListDto {
    private String table;
    private String key;
    private int partition;
    private int limit;
}