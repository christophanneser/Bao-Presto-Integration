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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class EffectiveOptimizerPart
{
    // Type can be RULE or OPTIMIZER
    private final OptimizerType type;
    // Name of the Rule or Optimizer class
    private final String name;
    // Precondition: the following optimizers must be disabled to make this optimizer effective
    private ImmutableSet<EffectiveOptimizerPart> optimizerDependencies;

    public EffectiveOptimizerPart(OptimizerType type, String name)
    {
        this.optimizerDependencies = ImmutableSet.of();
        this.type = type;
        this.name = name;
    }

    public OptimizerType getType()
    {
        return type;
    }

    public String getName()
    {
        return name;
    }

    public ImmutableSet<EffectiveOptimizerPart> getOptimizerDependencies()
    {
        return optimizerDependencies;
    }

    public void setOptimizerDependencies(ImmutableSet<EffectiveOptimizerPart> optimizerDependencies)
    {
        this.optimizerDependencies = optimizerDependencies;
    }

    // Default optimizers have no dependencies
    public boolean isDefaultOptimizer() { return optimizerDependencies.isEmpty(); }

    // Add dependencies from other rules (e.g. if this optimizer becomes effective only when dependencies are disabled)
    public void addDependencies(ImmutableSet<EffectiveOptimizerPart> dependencies)
    {
        optimizerDependencies = dependencies;
    }

    // Disable this optimizer and all its dependencies recursively
    public void disableOptimizerAndDependencies(OptimizerConfiguration optimizerConfiguration)
    {
        optimizerConfiguration.disableEffectiveOptimizerPart(this);
        optimizerDependencies.forEach(optimizerPart -> optimizerPart.disableOptimizerAndDependencies(optimizerConfiguration));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EffectiveOptimizerPart that = (EffectiveOptimizerPart) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }

    public JSONObject toJSON()
    {
        Map<String, Object> json = new HashMap<>();
        json.put("name", name);
        json.put("dependencies", optimizerDependencies.stream().map(opt -> opt.name).collect(Collectors.toList()));
        return new JSONObject(json);
    }
}
