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
public class CorrelationIdResponseFilter implements GlobalFilter {
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        log.debug("CorrelationId response called.");

        return chain.filter(exchange)
            .then(Mono.fromRunnable(() -> {
                log.debug("X-Correlation-ID found in Header, value: {}", exchange.getRequest().getHeaders().getFirst("X-Correlation-ID"));
                exchange.getResponse().getHeaders().add("X-Correlation-ID", exchange.getRequest().getHeaders().getFirst("X-Correlation-ID"));
            }));
    }
}
