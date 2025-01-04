package com.bteshome.apigateway.ratelimiter;

import lombok.Getter;

@Getter
public class Rule {
    private final String api;
    private final boolean isPerClient;
    private final Granularity granularity;
    private final int threshold;

    private Rule(String api, boolean isPerClient, Granularity granularity, int threshold) {
        this.api = api;
        this.isPerClient = isPerClient;
        this.granularity = granularity;
        this.threshold = threshold;
    }

    public static RuleBuilder builder(String api, Granularity granularity, int threshold) {
        if (api == null || api.isBlank()) {
            throw new RateLimiterException("Rule api must not be null.");
        }
        if (threshold <= 0) {
            throw new RateLimiterException("Rule threshold must be greater than 0.");
        }
        return new RuleBuilder(api, granularity, threshold);
    }

    public static class RuleBuilder {
        private final String api;
        private boolean isPerClient = false;
        private final Granularity granularity;
        private final int threshold;

        private RuleBuilder(String api, Granularity granularity, int threshold) {
            this.api = api;
            this.granularity = granularity;
            this.threshold = threshold;
        }

        public RuleBuilder isPerClient(boolean isPerClient) {
            this.isPerClient = isPerClient;
            return this;
        }

        public Rule build() {
            return new Rule(api, isPerClient, granularity, threshold);
        }
    }
}