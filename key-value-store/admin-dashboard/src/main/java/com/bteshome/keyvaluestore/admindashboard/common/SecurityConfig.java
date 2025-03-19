package com.bteshome.keyvaluestore.admindashboard.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
@Slf4j
public class SecurityConfig {
    @Autowired
    private AppSettings appSettings;

    @Bean
    SecurityFilterChain defaultSecurityFilterChain(HttpSecurity http) throws Exception {
        if (appSettings.isSecurityDisabled())
            return http.build();

        return http
                .authorizeHttpRequests( authorize -> authorize
                        .requestMatchers("/**").authenticated())
                .oauth2Login(httpSecurityOAuth2LoginConfigurer -> {})
                .build();
    }
}
