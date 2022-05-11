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

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.bao.BaoErrorCode;
import com.facebook.presto.sql.planner.bao.BaoException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

// Each Bao pipeline is associated with one table scan operator (todo: consider aggregations as well)
// and maps to a specific OptimizerConfiguration
public class BaoPipelines
{
    // Store the final hash of the current query plan here
    public int planHash;

    Map<String, OptimizerConfiguration> pipelineOptimizersConfigs;

    // disabledOptimizers: "<table>:<disabledOptimizer,...>!<table><disabledOptimizer,...>!..."
    // disabledRules: "<table>:<disabledRule,...>!<table><disabledRule,...>!..."
    public BaoPipelines(String disabledOptimizers, String disabledRules)
    {
        this.pipelineOptimizersConfigs = new HashMap<>();
        parsePipelineConfigs(disabledOptimizers, disabledRules);
    }

    // disabledOptimizerPerTable = <tableName>:<disabledOptimizers*>!...
    public void parsePipelineConfigs(String disabledOptimizersPerTable, String disabledRulesPerTable)
    {
        disableOptimizersOrRules(disabledOptimizersPerTable, OptimizerType.OPTIMIZER);
        disableOptimizersOrRules(disabledRulesPerTable, OptimizerType.RULE);
    }

    public void disableOptimizersOrRules(String disabledOptimizersPerTable, OptimizerType optimizerType)
    {
        String[] tableOptimizerConfigs = disabledOptimizersPerTable.isEmpty() ? new String[0] : disabledOptimizersPerTable.split("\\|");
        for (String tableOptimizerConfig : tableOptimizerConfigs) {
            String[] parts = tableOptimizerConfig.split(":");
            String tableName = parts[0];
            List<String> disabledOptimizers = (parts.length < 2 || parts[1].isEmpty()) ? new ArrayList<>() : Arrays.asList(parts[1].split(","));
            if (this.pipelineOptimizersConfigs.containsKey(tableName)) {
                // optimizer configuration for this pipeline already exist
                if (optimizerType == OptimizerType.OPTIMIZER) {
                    this.pipelineOptimizersConfigs.get(tableName).disableOptimizers(disabledOptimizers);
                }
                else {
                    this.pipelineOptimizersConfigs.get(tableName).disableRules(disabledOptimizers);
                }
            }
            else {
                // optimizer configuration for this pipeline does not yet exist
                OptimizerConfiguration optimizerConfiguration = new OptimizerConfiguration();

                if (optimizerType == OptimizerType.OPTIMIZER) {
                    optimizerConfiguration.disableOptimizers(disabledOptimizers);
                }
                else {
                    optimizerConfiguration.disableRules(disabledOptimizers);
                }
                this.pipelineOptimizersConfigs.put(tableName, optimizerConfiguration);
            }
        }
    }

    public Map<String, OptimizerConfiguration> getPipelineOptimizersConfigs()
    {
        return pipelineOptimizersConfigs;
    }

    private Map<String, OptimizerConfiguration> createPipelineOptimizerConfig(PlanNode root)
    {
        // find all table scans here
        Map<String, OptimizerConfiguration> pipelineOptimizerConfigs = new HashMap<>();
        Stack<PlanNode> nodes = new Stack<>();
        nodes.push(root);

        while (!nodes.empty()) {
            PlanNode current = nodes.pop();
            if (current instanceof TableScanNode) {
                TableScanNode tsn = (TableScanNode) current;
                // todo main challenge is that the connector object changes frequently (e.g. after predicate pushdowns), therefore we use it's string representation for now
                String tableRepresentation = tsn.getTable().getConnectorId().getCatalogName() + ":" + tsn.getTable().getConnectorHandle().toString();
                pipelineOptimizerConfigs.put(tableRepresentation, new OptimizerConfiguration());
            }
            else {
                for (PlanNode planNode : current.getSources()) {
                    nodes.push(planNode);
                }
            }
        }

        return pipelineOptimizerConfigs;
    }

    OptimizerConfiguration getPipeline(PlanNode node)
    {
        // in case a node is part of a join/aggregate pipeline, do not have a different pipeline there
        if (node instanceof TableScanNode) {
            TableScanNode tsn = (TableScanNode) node;
            String tableRepresentation = tsn.getTable().getConnectorId().getCatalogName() + ":" + tsn.getTable().getConnectorHandle().toString();
            return pipelineOptimizersConfigs.get(tableRepresentation);
        }

        else if (node.getSources().size() == 0) {
            // unexpected, throw
            throw new BaoException(BaoErrorCode.OPTIMIZER_PIPELINE_NOT_FOUND_ERROR, "could not find associated optimizer configuration pipeline");
        }
        else if (node.getSources().size() == 1) {
            return getPipeline(node.getSources().get(0));
        }
        else {
            // not yet implemented
            throw new BaoException(BaoErrorCode.JOINED_OPTIMIZER_PIPELINE_NOT_YET_IMPLEMENTED, "case not yet implemented where we consider separate pipelines for joins/aggregations/...");
        }
    }
}
