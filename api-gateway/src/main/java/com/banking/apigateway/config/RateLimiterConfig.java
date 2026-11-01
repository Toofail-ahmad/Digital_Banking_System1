package com.banking.apigateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import reactor.core.publisher.Mono;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;

import java.net.InetSocketAddress;

@Configuration
public class RateLimiterConfig {

    @Bean(name = {"keyResolver", "keySpaceResolver"})
    @Primary
    public KeyResolver keyResolver() {
        return exchange -> {
            InetSocketAddress remoteAddress =
                    exchange.getRequest().getRemoteAddress();

            if (remoteAddress != null && remoteAddress.getAddress() != null) {
                return Mono.just(
                        remoteAddress.getAddress().getHostAddress()
                );
            }

            return Mono.just("anonymous");
        };
    }
}
