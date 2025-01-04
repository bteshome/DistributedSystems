package com.bteshome.onlinestore.orderservice.model;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Entity
@Table(name = "line_items")
public class LineItem {
    @Id
    @GeneratedValue(
            strategy = GenerationType.TABLE,
            generator = "table-generator"
    )
    @TableGenerator(
            name =  "table-generator",
            table = "hibernate_seq",
            pkColumnName = "table_name",
            valueColumnName = "next_val",
            allocationSize = 5
    )
    private long id;
    private String skuCode;
    private int quantity;
    private BigDecimal price;

    @EqualsAndHashCode.Exclude
    @ManyToOne(fetch = FetchType.LAZY, optional = false, targetEntity = Order.class)
    @JoinColumn(name = "order_id", referencedColumnName = "id", nullable = false)
    private Order order;
}