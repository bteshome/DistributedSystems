package com.bteshome.keyvaluestore.adminclient.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Node {
    private String id;
    private String host;
    private int port;
    private int jmxPort;
    private String rack;
}
