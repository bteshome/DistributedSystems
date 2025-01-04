package com.bteshome.apigateway.routes;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Slf4j
//@Component
//@Order(2)
public class CorrelationIdRequestFilter implements GlobalFilter {
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        log.debug("CorrelationId request called.");

        if (exchange.getRequest().getHeaders().containsKey("X-Correlation-ID")) {
            log.debug("X-Correlation-ID found in Header, value: {}", exchange.getRequest().getHeaders().getFirst("X-Correlation-ID"));
        } else {
            String id = java.util.UUID.randomUUID().toString();
            log.debug("X-Correlation-ID generated: {}", id);
            exchange = exchange.mutate().request(exchange.getRequest().mutate().header("X-Correlation-ID", id).build()).build();
        }
        return chain.filter(exchange);
    }
}