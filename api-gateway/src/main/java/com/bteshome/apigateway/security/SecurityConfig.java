package com.bteshome.apigateway.security;

import com.bteshome.apigateway.common.AppSettings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

@Configuration
@EnableWebFluxSecurity
@Slf4j
public class SecurityConfig {
    @Autowired
    KeycloakRoleExtractor keycloakRoleExtractor;

    @Autowired
    AppSettings appSettings;

    @Bean
    SecurityWebFilterChain defaultSecurityFilterChain(ServerHttpSecurity http) throws Exception {
        if (appSettings.isSecurityDisabled()) {
            log.debug("Security is disabled.");
            return http.authorizeExchange( authorize -> authorize
                    .anyExchange().permitAll())
                    .csrf(ServerHttpSecurity.CsrfSpec::disable)
                    .build();
        };

        return http.authorizeExchange( authorize -> authorize
                .pathMatchers("/inventory/**").hasRole("customer")
                .pathMatchers("/orders/**").hasRole("admin")
                .anyExchange().permitAll())
                .oauth2ResourceServer(oAuth2ResourceServerSpec -> oAuth2ResourceServerSpec
                        .jwt(jwtSpec -> jwtSpec.jwtAuthenticationConverter(keycloakRoleExtractor.grantedAuthoritiesExtractor())))
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .build();
    }
}
