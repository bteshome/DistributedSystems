package com.bteshome.keyvaluestore.admindashboard.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ItemDeleteBatchDto {
    private String table;
    private List<String> itemkeys = new ArrayList<>();
}