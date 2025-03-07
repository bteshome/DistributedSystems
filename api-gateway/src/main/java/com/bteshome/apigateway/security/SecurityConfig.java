package com.bteshome.apigateway.security;

import com.bteshome.apigateway.common.AppSettings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.web.cors.CorsConfiguration;

import java.util.List;

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

        return http
                .authorizeExchange( authorize -> authorize
                    .pathMatchers("/orders/create/").authenticated()
                    .pathMatchers("/orders/query/").authenticated()
                    .pathMatchers("/inventory/products/").permitAll()
                    .pathMatchers("/").permitAll()
                    .pathMatchers("/public/**").permitAll()
                    .pathMatchers("/actuator/**").permitAll()
                    .anyExchange().denyAll())
                .oauth2ResourceServer(oAuth2ResourceServerSpec -> oAuth2ResourceServerSpec
                        .jwt(jwtSpec -> jwtSpec.jwtAuthenticationConverter(keycloakRoleExtractor.grantedAuthoritiesExtractor())))
                .cors(ServerHttpSecurity.CorsSpec::disable)
                .cors(cors -> cors.configurationSource(request -> {
                    CorsConfiguration config = new CorsConfiguration();
                    config.setAllowedOrigins(List.of(appSettings.getOrderingUiUrl()));
                    config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE"));
                    config.setAllowedHeaders(List.of("*"));
                    config.setAllowCredentials(true);
                    return config;
                }))
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .build();
    }
}
