package com.bteshome.keyvaluestore.client;

import com.bteshome.keyvaluestore.common.entities.Item;

public class Requests {

    public static void validateItem(Item item) {
        item.setKey(Validator.notEmpty(item.getKey(), "Key"));
        item.setValue(Validator.notEmpty(item.getValue(), "Value"));
        Validator.doesNotContain(item.getValue(), ",", "Value");
    }
}
