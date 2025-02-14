package com.bteshome.keyvaluestore.admindashboard.common;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {
    private final ClientMetadataRefresher clientMetadataRefresher;

    public WebConfig(ClientMetadataRefresher clientMetadataRefresher) {
        this.clientMetadataRefresher = clientMetadataRefresher;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(clientMetadataRefresher);
    }
}
