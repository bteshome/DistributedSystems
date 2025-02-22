package com.bteshome.onlinestore.admindashboard.dto;

import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class LineItem implements Serializable {
    private String skuCode;
    private int quantity;
    private BigDecimal price;

    public static LineItem fromString(String lineItem) {
        if (lineItem == null || lineItem.isEmpty())
            return null;

        String[] parts = lineItem.split("-");
        String skuCode = parts[0];
        int quantity = Integer.parseInt(parts[1]);
        BigDecimal price = BigDecimal.valueOf(Double.parseDouble(parts[2]));
        return new LineItem(skuCode, quantity, price);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LineItem lineItem = (LineItem) o;
        return quantity == lineItem.quantity && Objects.equals(skuCode, lineItem.skuCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(skuCode, quantity);
    }

    @Override
    public String toString() {
        return "%s-%s-%s".formatted(skuCode, quantity, price);
    }
}

