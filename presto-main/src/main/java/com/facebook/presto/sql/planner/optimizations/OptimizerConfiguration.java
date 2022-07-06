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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * This class stores the current optimizer configuration - e.g. which (non-iterative)-optimizers and rules are enabled
 * Please note: iterative optimizers are always enabled, but their associated rules can be turned on/off
 */
public class OptimizerConfiguration
{
    public static final ImmutableSet<String> ruleNames = new ImmutableSet.Builder<String>()
            .add("AddIntermediateAggregations")
            .add("AggregationExpressionRewrite")
            .add("AggregationRowExpressionRewrite")
            .add("ApplyExpressionRewrite")
            .add("ApplyConnectorOptimization")
            .add("ApplyRowExpressionRewrite")
            .add("Builder")
            .add("CanonicalizeExpressionRewriter")
            .add("CanonicalizeExpressions")
            .add("CheckNoPlanNodeMatchesRule")
            .add("Context")
            .add("CreatePartialTopN")
            .add("Decorrelated")
            .add("DecorrelatingVisitor")
            .add("DereferenceReplacer")
            .add("DesugarAtTimeZone")
            .add("DesugarCurrentUser")
            .add("DesugarLambdaExpression")
            .add("DesugarRowSubscript")
            .add("DesugarTryExpression")
            .add("DetermineJoinDistributionType")
            .add("DetermineSemiJoinDistributionType")
            .add("EliminateCrossJoins")
            .add("EliminateEmptyJoins")
            .add("EvaluateZeroLimit")
            .add("EvaluateZeroSample")
            .add("ExpressionRewriteRuleSet")
            .add("ExtractCommonPredicatesExpressionRewriter")
            .add("ExtractFromFilter")
            .add("ExtractFromJoin")
            .add("ExtractProjectDereferences")
            .add("ExtractSpatialInnerJoin")
            .add("ExtractSpatialJoins")
            .add("ExtractSpatialLeftJoin")
            .add("FilterExpressionRewrite")
            .add("FilterRowExpressionRewrite")
            .add("GatherAndMergeWindows")
            .add("ImplementBernoulliSampleAsFilter")
            .add("ImplementFilteredAggregations")
            .add("ImplementOffset")
            .add("InlineProjections")
            .add("InlineSqlFunctions")
            .add("InlineSqlFunctionsRewriter")
            .add("JoinDynamicFilterResult")
            .add("JoinEnumerationResult")
            .add("JoinEnumerator")
            .add("JoinExpressionRewrite")
            .add("JoinNodeFlattener")
            .add("JoinRowExpressionRewrite")
            .add("LambdaCaptureDesugaringRewriter")
            .add("LayoutConstraintEvaluatorForRowExpression")
            .add("LogicalExpressionRewriter")
            .add("LookupVariableResolver")
            .add("ManipulateAdjacentWindowsOverProjects")
            .add("MappedAggregationInfo")
            .add("MergeAdjacentWindowsOverProjects")
            .add("MergeFilters")
            .add("MergeLimitWithDistinct")
            .add("MergeLimitWithSort")
            .add("MergeLimitWithTopN")
            .add("MergeLimits")
            .add("MultiJoinNode")
            .add("MultipleDistinctAggregationToMarkDistinct")
            .add("PickTableLayout")
            .add("PickTableLayoutForPredicate")
            .add("PickTableLayoutWithoutPredicate")
            .add("PlanNodeWithCost")
            .add("PlanRemoteProjections")
            .add("PlanWithConsumedDynamicFilters")
            .add("PreconditionRules")
            .add("ProjectExpressionRewrite")
            .add("ProjectOffPushDownRule")
            .add("ProjectRowExpressionRewrite")
            .add("ProjectionContext")
            .add("PruneAggregationColumns")
            .add("PruneAggregationSourceColumns")
            .add("PruneCountAggregationOverScalar")
            .add("PruneCrossJoinColumns")
            .add("PruneFilterColumns")
            .add("PruneIndexSourceColumns")
            .add("PruneJoinChildrenColumns")
            .add("PruneJoinColumns")
            .add("PruneLimitColumns")
            .add("PruneMarkDistinctColumns")
            .add("PruneOrderByInAggregation")
            .add("PruneOutputColumns")
            .add("PruneProjectColumns")
            .add("PruneRedundantProjectionAssignments")
            .add("PruneSemiJoinColumns")
            .add("PruneSemiJoinFilteringSourceColumns")
            .add("PruneTableScanColumns")
            .add("PruneTopNColumns")
            .add("PruneValuesColumns")
            .add("PruneWindowColumns")
            .add("PushAggregationThroughOuterJoin")
            .add("PushDownDereferenceThrough")
            .add("PushDownDereferenceThroughJoin")
            .add("PushDownDereferenceThroughProject")
            .add("PushDownDereferenceThroughSemiJoin")
            .add("PushDownDereferenceThroughUnnest")
            .add("PushDownDereferences")
            .add("PushDownNegationsExpressionRewriter")
            .add("PushLimitThroughMarkDistinct")
            .add("PushLimitThroughOffset")
            .add("PushLimitThroughOuterJoin")
            .add("PushLimitThroughProject")
            .add("PushLimitThroughSemiJoin")
            .add("PushLimitThroughUnion")
            .add("PushOffsetThroughProject")
            .add("PushPartialAggregationThroughExchange")
            .add("PushPartialAggregationThroughJoin")
            .add("PushProjectionThroughExchange")
            .add("PushProjectionThroughUnion")
            .add("PushRemoteExchangeThroughAssignUniqueId")
            .add("PushTableWriteThroughUnion")
            .add("PushTopNThroughUnion")
            .add("PushdownDereferencesInProject")
            .add("RemoveEmptyDelete")
            .add("RemoveFullSample")
            .add("RemoveRedundantIdentityProjections")
            .add("RemoveTrivialFilters")
            .add("RemoveUnreferencedScalarApplyNodes")
            .add("RemoveUnreferencedScalarLateralNodes")
            .add("RemoveUnsupportedDynamicFilters")
            .add("ReorderJoins")
            .add("RewriteAggregationIfToFilter")
            .add("RewriteFilterWithExternalFunctionToProject")
            .add("RewriteSpatialPartitioningAggregation")
            .add("Rewriter")
            .add("RowExpressionRewriteRuleSet")
            .add("RuntimeReorderJoinSides")
            .add("SimplifyCardinalityMap")
            .add("SimplifyCountOverConstant")
            .add("SimplifyExpressions")
            .add("SimplifyRowExpressions")
            .add("SingleDistinctAggregationToGroupBy")
            .add("SpatialJoinRowExpressionRewrite")
            .add("SwapAdjacentWindowsBySpecifications")
            .add("TableFinishRowExpressionRewrite")
            .add("TableWriterRowExpressionRewrite")
            .add("TransformCorrelatedInPredicateToJoin")
            .add("TransformCorrelatedLateralJoinToJoin")
            .add("TransformCorrelatedScalarAggregationToJoin")
            .add("TransformCorrelatedScalarSubquery")
            .add("TransformCorrelatedSingleRowSubqueryToProject")
            .add("TransformExistsApplyToLateralNode")
            .add("TransformUncorrelatedInPredicateSubqueryToSemiJoin")
            .add("TransformUncorrelatedLateralToJoin")
            .add("TranslateExpressions")
            .add("ValuesExpressionRewrite")
            .add("ValuesRowExpressionRewrite")
            .add("Visitor")
            .add("WindowRowExpressionRewrite")
            .add("RemoveRedundantAggregateDistinct")
            .add("RemoveRedundantDistinct")
            .build();
    public static final ImmutableSet<String> optimizerNames = new ImmutableSet.Builder<String>()
            .add("AddLocalExchanges")
            .add("ApplyConnectorOptimization")
            .add("CheckSubqueryNodesAreRewritten")
            .add("HashGenerationOptimizer")
            .add("HashBasedPartialDistinctLimit")
            .add("ImplementIntersectAndExceptAsUnion")
            .add("IndexJoinOptimizer")
            .add("IterativeOptimizer") // <-- will always be enabled!
            .add("KeyBasedSampler")
            .add("LimitPushDown")
            .add("MergeJoinOptimizer")
            .add("MetadataDeleteOptimizer")
            .add("MetadataQueryOptimizer")
            .add("OptimizeMixedDistinctAggregations")
            .add("PruneUnreferencedOutputs")
            .add("PushdownSubfields")
            .add("RemoveUnsupportedDynamicFilters")
            .add("ReplicateSemiJoinInDelete")
            .add("SetFlatteningOptimizer")
            .add("StatsRecordingPlanOptimizer")
            .add("TransformQuantifiedComparisonApplyToLateralJoin")
            .add("UnaliasSymbolReferences")
            .add("WindowFilterPushDown")
            .build();
    private static final Logger log = Logger.get(OptimizerConfiguration.class);
    public boolean appliedCurrentOptimizer;
    // Store the final hash of the current query plan here
    public int planHash;
    // en/disable optimizers and rules
    private Map<String, Boolean> optimizersEnabled = createConfigMapFromList();
    // track optimizers and rules if they are required or effective
    private Set<EffectiveOptimizerPart> effectiveOptimizers;

    public OptimizerConfiguration()
    {
        effectiveOptimizers = new HashSet<>();
    }

    public OptimizerConfiguration(String disabledOptimizers)
    {
        effectiveOptimizers = new HashSet<>();
        this.disableOptimizers(disabledOptimizers.isEmpty() ? new ArrayList<>() : Arrays.asList(disabledOptimizers.split(",")));
    }

    private static Map<String, Boolean> createConfigMapFromList()
    {
        Map<String, Boolean> map = new HashMap<>();
        OptimizerConfiguration.optimizerNames.forEach(name -> map.put(name, true));
        OptimizerConfiguration.ruleNames.forEach(name -> map.put(name, true));
        return map;
    }

    public Set<EffectiveOptimizerPart> getEffectiveOptimizers()
    {
        return effectiveOptimizers;
    }

    public Boolean isOptimizerEnabled(String optimizer)
    {
        if (!optimizersEnabled.containsKey(optimizer)) {
            log.error("optimizer %s does not exist in OptimizerConfiguration", optimizer);
            return true;
        }
        return optimizersEnabled.get(optimizer);
    }

    public void registerEffectiveRule(String rule)
    {
        assert (effectiveOptimizers != null);
        effectiveOptimizers.add(new EffectiveOptimizerPart(OptimizerType.RULE, rule));
    }

    public void registerEffectiveOptimizer(String optimizer)
    {
        assert (effectiveOptimizers != null);
        effectiveOptimizers.add(new EffectiveOptimizerPart(OptimizerType.OPTIMIZER, optimizer));
    }

    public void reset()
    {
        effectiveOptimizers = new HashSet<>();
        optimizersEnabled = createConfigMapFromList();
    }

    public void disableOptimizers(List<String> optimizers)
    {
        optimizersEnabled = createConfigMapFromList();
        for (String optimizer : optimizers) {
            assert (optimizersEnabled.containsKey(optimizer));
            optimizersEnabled.replace(optimizer, false);
        }
    }

    public void disableEffectiveOptimizerPart(EffectiveOptimizerPart optimizerPart)
    {
        disableOptimizer(optimizerPart.getName());
    }

    public void disableOptimizer(String optimizer)
    {
        optimizersEnabled = createConfigMapFromList();
        if (!optimizersEnabled.containsKey(optimizer)) {
            System.out.println("ERROR: optimizer not found:" + optimizer);
        }
        optimizersEnabled.replace(optimizer, false);
    }

    public String getJson(PlanNode root)
    {
        String result;
        result = root.toString();
        ObjectMapper om = new ObjectMapper();
        try {
            result = om.writeValueAsString(root);
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public int hashPlan(PlanNode root)

    {
        DeltaTracker delta = new DeltaTracker();
        delta.visitPlan(root, 33);
        return delta.state;
    }

    /// Helper class to track changing query plans
    private static class DeltaTracker
            extends InternalPlanVisitor<PlanNode, Integer>
    {
        private int state = 73;

        @Override
        public PlanNode visitPlan(PlanNode node, Integer context)
        {
            node.getSources().forEach(child -> visitPlan(child, state));
            state += node.hashCode();
            return node;
        }
    }
}
