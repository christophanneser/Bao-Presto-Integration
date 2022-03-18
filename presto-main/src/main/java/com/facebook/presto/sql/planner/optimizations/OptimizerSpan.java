package com.facebook.presto.sql.planner.optimizations;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

// Stores the optimizer span
public class OptimizerSpan
{
    // remember all seen optimizers and rules
    private final Set<String> seenOptimizers;
    private final Set<String> seenRules;

    // keep queue of optimizers that should be deactivated
    private final Queue<String> effectiveOptimizers;
    private final Queue<String> effectiveRules;

    // initialize using the default effective rules/optimizers
    public OptimizerSpan(Set<String> effectiveOptimizers, Set<String> effectiveRules)
    {
        this.seenOptimizers = new HashSet<>(effectiveOptimizers);
        this.seenRules = new HashSet<>(effectiveRules);

        this.effectiveOptimizers = new ConcurrentLinkedQueue<>(effectiveOptimizers);
        this.effectiveRules = new ConcurrentLinkedQueue<>(effectiveRules);
    }

    public boolean hasNext()
    {
        return !(effectiveOptimizers.isEmpty() && effectiveRules.isEmpty());
    }

    public EffectiveOptimizerPart next()
    {
        if (!effectiveRules.isEmpty()) {
            return EffectiveOptimizerPart.getRule(effectiveRules.poll());
        }
        assert (!effectiveOptimizers.isEmpty());
        return EffectiveOptimizerPart.getOptimizer(effectiveOptimizers.poll());
    }

    public void addRule(String rule)
    {
        if (!seenRules.contains(rule)) {
            seenRules.add(rule);
            effectiveRules.offer(rule);
        }
    }

    public void addOptimizer(String optimizer)
    {
        if (!seenOptimizers.contains(optimizer)) {
            seenOptimizers.add(optimizer);
            effectiveOptimizers.offer(optimizer);
        }
    }

    public void addOptimizers(Set<String> effectiveOptimizers)
    {
        effectiveOptimizers.forEach(this::addOptimizer);
    }

    public void addRules(Set<String> effectiveRules)
    {
        effectiveRules.forEach(this::addRule);
    }

    public static class EffectiveOptimizerPart
    {
        public OptimizerType type;
        public String name;

        EffectiveOptimizerPart(OptimizerType type, String name)
        {
            this.type = type;
            this.name = name;
        }

        public static EffectiveOptimizerPart getOptimizer(String optimizer)
        {
            return new EffectiveOptimizerPart(OptimizerType.OPTIMIZER, optimizer);
        }

        public static EffectiveOptimizerPart getRule(String rule)
        {
            return new EffectiveOptimizerPart(OptimizerType.RULE, rule);
        }
    }
}
