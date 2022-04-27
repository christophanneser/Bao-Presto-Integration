/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.optimizations;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

// Stores the optimizer span
public class QuerySpan
{
    // remember all seen optimizers and rules
    private final Set<String> seenOptimizers;
    private final Set<String> seenRules;

    // keep queue of optimizers that should be deactivated
    private final Queue<String> effectiveOptimizers;
    private final Queue<String> effectiveRules;

    // initialize using the default effective rules/optimizers
    public QuerySpan(Set<String> effectiveOptimizers, Set<String> effectiveRules)
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
