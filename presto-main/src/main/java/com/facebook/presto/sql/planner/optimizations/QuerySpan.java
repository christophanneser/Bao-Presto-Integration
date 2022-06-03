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

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

// Stores the optimizer span
public class QuerySpan
{
    // remember all seen optimizers and rules
    private final Set<EffectiveOptimizerPart> seenOptimizers;
    // keep queue of optimizers that should be deactivated
    private final Queue<EffectiveOptimizerPart> queuedOptimizers;

    // initialize using the default effective rules/optimizers
    public QuerySpan(Set<EffectiveOptimizerPart> effectiveOptimizers)
    {
        this.seenOptimizers = new HashSet<>(effectiveOptimizers);
        this.queuedOptimizers = new ConcurrentLinkedQueue<>(effectiveOptimizers);
    }

    public boolean hasNext()
    {
        return !queuedOptimizers.isEmpty();
    }

    public EffectiveOptimizerPart next()
    {
        return queuedOptimizers.poll();
    }

    public Collection<EffectiveOptimizerPart> getSeenOptimizers() { return seenOptimizers; }

    // Add the newly detected, alternative rules and optimizers and keep track of the rules and optimizers that have been disabled before
    public void addAlternativeRulesAndOptimizers(ImmutableSet<EffectiveOptimizerPart> disabledRulesAndOptimizers, Set<EffectiveOptimizerPart> alternativeROs)
    {
        // remove already known rules and optimizers
        alternativeROs.removeAll(seenOptimizers);
        // add optimizer dependencies
        alternativeROs.forEach(eop -> eop.addDependencies(disabledRulesAndOptimizers));
        // add new alternative optimizers to queue
        queuedOptimizers.addAll(alternativeROs);
        // add new alternatives to seenOptimizers
        seenOptimizers.addAll(alternativeROs);
    }
}
