package com.bteshome.ratelimiterrulesdashboard.repository;

import com.bteshome.ratelimiterrulesdashboard.model.Rule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RuleRepository extends JpaRepository<Rule, Long> {
}