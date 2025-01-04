package com.bteshome.onlinestore.inventoryservice.model;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Entity
@Table(name = "products")
public class Product {
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
    private String name;
    private String description;
    private BigDecimal price;
    private String skuCode;
    @Column(name = "stock_level")
    private int stockLevel;
}