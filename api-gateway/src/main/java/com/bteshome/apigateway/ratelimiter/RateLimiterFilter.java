package com.bteshome.apigateway.ratelimiter;

import com.bteshome.apigateway.common.AppSettings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@ConditionalOnProperty(name = "rate-limiter-disabled", havingValue = "false", matchIfMissing = true)
@Order(-100)
public class RateLimiterFilter implements GlobalFilter {
    @Autowired
    RateLimiter limiter;

    @Autowired
    AppSettings appSettings;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // TODO
        String path = exchange.getRequest().getPath().toString();
        String api = path.split("/")[1];
        System.out.println(api);
        /*var client = exchange.getRequest().getRemoteAddress().getAddress();

        if (limiter.tryAcquire(api, client.toString())) {
            log.debug("RateLimiter allowed caller: {}", exchange.getRequest().getRemoteAddress().getAddress());
            return chain.filter(exchange);
        } else {
            log.debug("RateLimiter blocked caller: {}", exchange.getRequest().getRemoteAddress().getAddress());
            exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
            return exchange.getResponse().setComplete();
        }*/

        return chain.filter(exchange);
    }
}