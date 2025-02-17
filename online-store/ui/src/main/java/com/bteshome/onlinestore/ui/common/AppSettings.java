package com.bteshome.onlinestore.ui.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AppSettings {
    private String inventoryServiceUrl;
    private String orderServiceUrl;
}
