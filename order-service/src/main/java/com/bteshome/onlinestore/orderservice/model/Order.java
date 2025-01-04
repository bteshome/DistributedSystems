package com.bteshome.onlinestore.orderservice.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Entity
@Table(name = "orders")
public class Order {
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
    private String orderNumber;
    private String email;
    private String notificationStatus;
    @EqualsAndHashCode.Exclude
    @OneToMany(fetch = FetchType.EAGER, mappedBy = "order", cascade = CascadeType.ALL, targetEntity = LineItem.class)
    private List<LineItem> lineItems;
}