package com.bteshome.ratelimiterrulesdashboard.model;

import com.bteshome.ratelimiterrulesdashboard.Granularity;
import com.bteshome.ratelimiterrulesdashboard.RateLimiterRuleException;
import jakarta.persistence.*;
import lombok.*;


@Getter
@Setter
@Builder
@Entity
@Table(name = "rules")
public class Rule {
    @Id
    @GeneratedValue(
            strategy = GenerationType.TABLE,
            generator = "table-generator"
    )
    @TableGenerator(
            name =  "table-generator",
            table = "hibernate_seq",
            pkColumnName = "table_name",
            valueColumnName = "next_val",
            allocationSize = 5
    )
    private long id;
    private String api;
    @Column(name = "is_per_client")
    private boolean isPerClient;
    private String granularity;
    private int threshold;

    public Rule() {
    }

    public Rule(String api, boolean isPerClient, String granularity, int threshold) {
        this.api = api;
        this.isPerClient = isPerClient;
        this.granularity = granularity;
        this.threshold = threshold;
    }

    public static Rule.RuleBuilder builder(String api, Granularity granularity, int threshold) {
        if (api == null || api.isBlank()) {
            throw new RateLimiterRuleException("Rule api must not be null.");
        }
        if (threshold <= 0) {
            throw new RateLimiterRuleException("Rule threshold must be greater than 0.");
        }
        return new Rule.RuleBuilder(api, granularity, threshold);
    }

    public static class RuleBuilder {
        private String api;
        private boolean isPerClient = false;
        private Granularity granularity;
        private int threshold;

        private RuleBuilder(String api, Granularity granularity, int threshold) {
            this.api = api;
            this.granularity = granularity;
            this.threshold = threshold;
        }

        public Rule.RuleBuilder isPerClient(boolean isPerClient) {
            this.isPerClient = isPerClient;
            return this;
        }

        public Rule build() {
            return new Rule(api, isPerClient, granularity.toString(), threshold);
        }
    }
}