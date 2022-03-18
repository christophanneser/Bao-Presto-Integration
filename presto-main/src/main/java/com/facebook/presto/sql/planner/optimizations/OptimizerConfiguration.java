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
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * This class stores the current optimizer configuration - e.g. which (non-iterative)-optimizers and rules are enabled
 * Please note: iterative optimizers are always enabled, but their associated rules can be turned on/off
 */
public class OptimizerConfiguration
{
    public boolean appliedCurrentOptimizer;

    // Store the final hash of the current query plan here
    public int planHash;

    // en/disable optimizers and rules
    public Map<String, Boolean> optimizersEnabled = createConfigMapFromList(optimizerNames());
    public Map<String, Boolean> rulesEnabled = createConfigMapFromList(ruleNames());

    // track optimizers and rules if they are required or effective
    public Set<String> effectiveOptimizers;
    public Set<String> effectiveRules;

    public static Map<String, Integer> createRulesHitsMap()
    {
        Map<String, Integer> map = new HashMap<>();
        for (String ruleName : ruleNames()) {
            map.put(ruleName, 0);
        }
        return map;
    }

    private static List<String> optimizerNames()
    {
        List<String> opts = new ArrayList<>();
        opts.add("AddLocalExchanges");
        opts.add("ApplyConnectorOptimization");
        opts.add("CheckSubqueryNodesAreRewritten");
        opts.add("HashGenerationOptimizer");
        opts.add("ImplementIntersectAndExceptAsUnion");
        opts.add("IndexJoinOptimizer");
        opts.add("IterativeOptimizer"); // <-- will always be enabled!
        opts.add("KeyBasedSampler");
        opts.add("LimitPushDown");
        opts.add("MetadataDeleteOptimizer");
        opts.add("MetadataQueryOptimizer");
        opts.add("OptimizeMixedDistinctAggregations");
        opts.add("PruneUnreferencedOutputs");
        opts.add("PushdownSubfields");
        opts.add("RemoveUnsupportedDynamicFilters");
        opts.add("ReplicateSemiJoinInDelete");
        opts.add("SetFlatteningOptimizer");
        opts.add("StatsRecordingPlanOptimizer");
        opts.add("TransformQuantifiedComparisonApplyToLateralJoin");
        opts.add("UnaliasSymbolReferences");
        opts.add("WindowFilterPushDown");
        return opts;
    }

    public static List<String> ruleNames()
    {
        List<String> rules = new ArrayList<>();

        rules.add("AddIntermediateAggregations");
        rules.add("AggregationExpressionRewrite");
        rules.add("AggregationRowExpressionRewrite");
        rules.add("ApplyExpressionRewrite");
        rules.add("ApplyConnectorOptimization");
        rules.add("ApplyRowExpressionRewrite");
        rules.add("Builder");
        rules.add("CanonicalizeExpressionRewriter");
        rules.add("CanonicalizeExpressions");
        rules.add("CheckNoPlanNodeMatchesRule");
        rules.add("Context");
        rules.add("CreatePartialTopN");
        rules.add("Decorrelated");
        rules.add("DecorrelatingVisitor");
        rules.add("DereferenceReplacer");
        rules.add("DesugarAtTimeZone");
        rules.add("DesugarCurrentUser");
        rules.add("DesugarLambdaExpression");
        rules.add("DesugarRowSubscript");
        rules.add("DesugarTryExpression");
        rules.add("DetermineJoinDistributionType");
        rules.add("DetermineSemiJoinDistributionType");
        rules.add("EliminateCrossJoins");
        rules.add("EliminateEmptyJoins");
        rules.add("EvaluateZeroLimit");
        rules.add("EvaluateZeroSample");
        rules.add("ExpressionRewriteRuleSet");
        rules.add("ExtractCommonPredicatesExpressionRewriter");
        rules.add("ExtractFromFilter");
        rules.add("ExtractFromJoin");
        rules.add("ExtractProjectDereferences");
        rules.add("ExtractSpatialInnerJoin");
        rules.add("ExtractSpatialJoins");
        rules.add("ExtractSpatialLeftJoin");
        rules.add("FilterExpressionRewrite");
        rules.add("FilterRowExpressionRewrite");
        rules.add("GatherAndMergeWindows");
        rules.add("ImplementBernoulliSampleAsFilter");
        rules.add("ImplementFilteredAggregations");
        rules.add("ImplementOffset");
        rules.add("InlineProjections");
        rules.add("InlineSqlFunctions");
        rules.add("InlineSqlFunctionsRewriter");
        rules.add("JoinDynamicFilterResult");
        rules.add("JoinEnumerationResult");
        rules.add("JoinEnumerator");
        rules.add("JoinExpressionRewrite");
        rules.add("JoinNodeFlattener");
        rules.add("JoinRowExpressionRewrite");
        rules.add("LambdaCaptureDesugaringRewriter");
        rules.add("LayoutConstraintEvaluatorForRowExpression");
        rules.add("LogicalExpressionRewriter");
        rules.add("LookupVariableResolver");
        rules.add("ManipulateAdjacentWindowsOverProjects");
        rules.add("MappedAggregationInfo");
        rules.add("MergeAdjacentWindowsOverProjects");
        rules.add("MergeFilters");
        rules.add("MergeLimitWithDistinct");
        rules.add("MergeLimitWithSort");
        rules.add("MergeLimitWithTopN");
        rules.add("MergeLimits");
        rules.add("MultiJoinNode");
        rules.add("MultipleDistinctAggregationToMarkDistinct");
        rules.add("PickTableLayout");
        rules.add("PickTableLayoutForPredicate");
        rules.add("PickTableLayoutWithoutPredicate");
        rules.add("PlanNodeWithCost");
        rules.add("PlanRemotePojections");
        rules.add("PlanWithConsumedDynamicFilters");
        rules.add("PreconditionRules");
        rules.add("ProjectExpressionRewrite");
        rules.add("ProjectOffPushDownRule");
        rules.add("ProjectRowExpressionRewrite");
        rules.add("ProjectionContext");
        rules.add("PruneAggregationColumns");
        rules.add("PruneAggregationSourceColumns");
        rules.add("PruneCountAggregationOverScalar");
        rules.add("PruneCrossJoinColumns");
        rules.add("PruneFilterColumns");
        rules.add("PruneIndexSourceColumns");
        rules.add("PruneJoinChildrenColumns");
        rules.add("PruneJoinColumns");
        rules.add("PruneLimitColumns");
        rules.add("PruneMarkDistinctColumns");
        rules.add("PruneOrderByInAggregation");
        rules.add("PruneOutputColumns");
        rules.add("PruneProjectColumns");
        rules.add("PruneRedundantProjectionAssignments");
        rules.add("PruneSemiJoinColumns");
        rules.add("PruneSemiJoinFilteringSourceColumns");
        rules.add("PruneTableScanColumns");
        rules.add("PruneTopNColumns");
        rules.add("PruneValuesColumns");
        rules.add("PruneWindowColumns");
        rules.add("PushAggregationThroughOuterJoin");
        rules.add("PushDownDereferenceThrough");
        rules.add("PushDownDereferenceThroughJoin");
        rules.add("PushDownDereferenceThroughProject");
        rules.add("PushDownDereferenceThroughSemiJoin");
        rules.add("PushDownDereferenceThroughUnnest");
        rules.add("PushDownDereferences");
        rules.add("PushDownNegationsExpressionRewriter");
        rules.add("PushLimitThroughMarkDistinct");
        rules.add("PushLimitThroughOffset");
        rules.add("PushLimitThroughOuterJoin");
        rules.add("PushLimitThroughProject");
        rules.add("PushLimitThroughSemiJoin");
        rules.add("PushLimitThroughUnion");
        rules.add("PushOffsetThroughProject");
        rules.add("PushPartialAggregationThroughExchange");
        rules.add("PushPartialAggregationThroughJoin");
        rules.add("PushProjectionThroughExchange");
        rules.add("PushProjectionThroughUnion");
        rules.add("PushRemoteExchangeThroughAssignUniqueId");
        rules.add("PushTableWriteThroughUnion");
        rules.add("PushTopNThroughUnion");
        rules.add("PushdownDereferencesInProject");
        rules.add("RemoveEmptyDelete");
        rules.add("RemoveFullSample");
        rules.add("RemoveRedundantIdentityProjections");
        rules.add("RemoveTrivialFilters");
        rules.add("RemoveUnreferencedScalarApplyNodes");
        rules.add("RemoveUnreferencedScalarLateralNodes");
        rules.add("RemoveUnsupportedDynamicFilters");
        rules.add("ReorderJoins");
        rules.add("RewriteAggregationIfToFilter");
        rules.add("RewriteFilterWithExternalFunctionToProject");
        rules.add("RewriteSpatialPartitioningAggregation");
        rules.add("Rewriter");
        rules.add("Rewriter");
        rules.add("RowExpressionRewriteRuleSet");
        rules.add("RuntimeReorderJoinSides");
        rules.add("SimplifyCountOverConstant");
        rules.add("SimplifyExpressions");
        rules.add("SimplifyRowExpressions");
        rules.add("SingleDistinctAggregationToGroupBy");
        rules.add("SpatialJoinRowExpressionRewrite");
        rules.add("SwapAdjacentWindowsBySpecifications");
        rules.add("TableFinishRowExpressionRewrite");
        rules.add("TableWriterRowExpressionRewrite");
        rules.add("TransformCorrelatedInPredicateToJoin");
        rules.add("TransformCorrelatedLateralJoinToJoin");
        rules.add("TransformCorrelatedScalarAggregationToJoin");
        rules.add("TransformCorrelatedScalarSubquery");
        rules.add("TransformCorrelatedSingleRowSubqueryToProject");
        rules.add("TransformExistsApplyToLateralNode");
        rules.add("TransformUncorrelatedInPredicateSubqueryToSemiJoin");
        rules.add("TransformUncorrelatedLateralToJoin");
        rules.add("TranslateExpressions");
        rules.add("ValuesExpressionRewrite");
        rules.add("ValuesRowExpressionRewrite");
        rules.add("Visitor");
        rules.add("WindowRowExpressionRewrite");

        return rules;
    }

    private static Map<String, Boolean> createConfigMapFromList(List<String> optimizersOrRules)
    {
        Map<String, Boolean> map = new HashMap<>();
        for (String ruleName : optimizersOrRules) {
            map.put(ruleName, true); // all optimizers/rules are enabled by default
        }
        return map;
    }

    public void registerRuleHit(String ruleName)
    {
        //if (SqlQueryExecution.getQuerySpan) { todo
        assert (effectiveRules != null);
        effectiveRules.add(ruleName);
        //}
    }

    public void reset()
    {
        optimizersEnabled = createConfigMapFromList(optimizerNames());
        rulesEnabled = createConfigMapFromList(ruleNames());
    }

    public void disableOptimizers(List<String> optimizers)
    {
        reset();
        System.out.println("disable optimizers ...");
        for (String optimizer : optimizers) {
            assert (optimizersEnabled.containsKey(optimizer));
            optimizersEnabled.replace(optimizer, false);
        }
    }

    public void disableOptimizer(String optimizer)
    {
        reset();
        if (!optimizersEnabled.containsKey(optimizer)) {
            System.out.println("ERROR: optimizer not found:" + optimizer);
        }
        assert (optimizersEnabled.containsKey(optimizer));
        optimizersEnabled.replace(optimizer, false);
    }

    public void disableRule(String rule)
    {
        reset();
        if (!rulesEnabled.containsKey(rule)) {
            System.out.println("ERROR: rule not found:" + rule);
        }
        assert (rulesEnabled.containsKey(rule));
        rulesEnabled.replace(rule, false);
    }

    public void disableRules(List<String> rules)
    {
        reset();
        for (String rule : rules) {
            assert (rulesEnabled.containsKey(rule));
            rulesEnabled.replace(rule, false);
        }
    }

    public String getJson(PlanNode root)
    {
        String result = "";
        result = root.toString();
        ObjectMapper om = new ObjectMapper();
        try {
            result = om.writeValueAsString(root);
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        // JsonCodec<PlanNode> codec = new JsonCodecFactory().jsonCodec(PlanNode.class);
        // result = codec.toJson(root);
        // System.out.println(result);
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
            node.getSources().forEach(child -> visitPlan((PlanNode) child, state));
            state += node.hashCode();
            return node;
        }
    }
}

