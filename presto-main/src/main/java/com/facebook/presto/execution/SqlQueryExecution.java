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
package com.facebook.presto.execution;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.scheduler.ExecutionPolicy;
import com.facebook.presto.execution.scheduler.LegacySqlQueryScheduler;
import com.facebook.presto.execution.scheduler.SectionExecutionFactory;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.execution.scheduler.SqlQueryScheduler;
import com.facebook.presto.execution.scheduler.SqlQuerySchedulerInterface;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import com.facebook.presto.split.CloseableSplitSourceProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.InputExtractor;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.OutputExtractor;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.bao.BaoConnector;
import com.facebook.presto.sql.planner.optimizations.EffectiveOptimizerPart;
import com.facebook.presto.sql.planner.optimizations.OptimizerConfiguration;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.QuerySpan;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.tree.Explain;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getExecutionPolicy;
import static com.facebook.presto.SystemSessionProperties.getQueryAnalyzerTimeout;
import static com.facebook.presto.SystemSessionProperties.isExecuteQuery;
import static com.facebook.presto.SystemSessionProperties.isExportGraphviz;
import static com.facebook.presto.SystemSessionProperties.isExportJSON;
import static com.facebook.presto.SystemSessionProperties.isSpoolingOutputBufferEnabled;
import static com.facebook.presto.SystemSessionProperties.isUseLegacyScheduler;
import static com.facebook.presto.common.RuntimeMetricName.FRAGMENT_PLAN_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.LOGICAL_PLANNER_TIME_NANOS;
import static com.facebook.presto.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.buffer.OutputBuffers.createSpoolingOutputBuffers;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.ParameterUtils.parameterExtractor;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private static final OutputBufferId OUTPUT_BUFFER_ID = new OutputBufferId(0);

    private final QueryStateMachine stateMachine;
    private final String slug;
    private final int retryCount;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final SplitManager splitManager;
    private final List<PlanOptimizer> planOptimizers;
    private final List<PlanOptimizer> runtimePlanOptimizers;
    private final PlanFragmenter planFragmenter;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final ExecutorService queryExecutor;
    private final ScheduledExecutorService timeoutThreadExecutor;
    private final SectionExecutionFactory sectionExecutionFactory;
    private final InternalNodeManager internalNodeManager;

    private final AtomicReference<SqlQuerySchedulerInterface> queryScheduler = new AtomicReference<>();
    private final AtomicReference<Plan> queryPlan = new AtomicReference<>();
    private final ExecutionPolicy executionPolicy;
    private final SplitSchedulerStats schedulerStats;
    private final Analysis analysis;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final PlanChecker planChecker;
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private final AtomicReference<PlanVariableAllocator> variableAllocator = new AtomicReference<>();
    private final PartialResultQueryManager partialResultQueryManager;
    private final AtomicReference<Optional<ResourceGroupQueryLimits>> resourceGroupQueryLimits = new AtomicReference<>(Optional.empty());
    private final BaoConnector baoConnector;

    private SqlQueryExecution(
            PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            String slug,
            int retryCount,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            SplitManager splitManager,
            List<PlanOptimizer> planOptimizers,
            List<PlanOptimizer> runtimePlanOptimizers,
            PlanFragmenter planFragmenter,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            ExecutorService queryExecutor,
            ScheduledExecutorService timeoutThreadExecutor,
            SectionExecutionFactory sectionExecutionFactory,
            InternalNodeManager internalNodeManager,
            QueryExplainer queryExplainer,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector,
            PlanChecker planChecker,
            PartialResultQueryManager partialResultQueryManager)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            this.slug = requireNonNull(slug, "slug is null");
            this.retryCount = retryCount;
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
            this.runtimePlanOptimizers = requireNonNull(runtimePlanOptimizers, "runtimePlanOptimizers is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.timeoutThreadExecutor = requireNonNull(timeoutThreadExecutor, "timeoutThreadExecutor is null");
            this.sectionExecutionFactory = requireNonNull(sectionExecutionFactory, "sectionExecutionFactory is null");
            this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
            this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
            this.planChecker = requireNonNull(planChecker, "planChecker is null");
            this.baoConnector = new BaoConnector(stateMachine.getSession());

            // analyze query
            requireNonNull(preparedQuery, "preparedQuery is null");

            stateMachine.beginSemanticAnalyzing();
            Analyzer analyzer = new Analyzer(
                    stateMachine.getSession(),
                    metadata,
                    sqlParser,
                    accessControl,
                    Optional.of(queryExplainer),
                    preparedQuery.getParameters(),
                    parameterExtractor(preparedQuery.getStatement(), preparedQuery.getParameters()),
                    warningCollector);

            this.analysis = analyzer.analyzeSemantic(preparedQuery.getStatement(), false);
            stateMachine.setUpdateType(analysis.getUpdateType());
            stateMachine.setExpandedQuery(analysis.getExpandedQuery());

            stateMachine.beginColumnAccessPermissionChecking();
            analyzer.checkColumnAccessPermissions(this.analysis);
            stateMachine.endColumnAccessPermissionChecking();

            // when the query finishes cache the final query info, and clear the reference to the output stage
            AtomicReference<SqlQuerySchedulerInterface> queryScheduler = this.queryScheduler;
            stateMachine.addStateChangeListener(state -> {
                if (!state.isDone()) {
                    return;
                }

                // query is now done, so abort any work that is still running
                SqlQuerySchedulerInterface scheduler = queryScheduler.get();
                if (scheduler != null) {
                    scheduler.abort();
                }
            });

            this.remoteTaskFactory = new TrackingRemoteTaskFactory(requireNonNull(remoteTaskFactory, "remoteTaskFactory is null"), stateMachine);
            this.partialResultQueryManager = requireNonNull(partialResultQueryManager, "partialResultQueryManager is null");
        }
    }

    private static Set<ConnectorId> extractConnectors(Analysis analysis)
    {
        ImmutableSet.Builder<ConnectorId> connectors = ImmutableSet.builder();

        for (TableHandle tableHandle : analysis.getTables()) {
            connectors.add(tableHandle.getConnectorId());
        }

        if (analysis.getInsert().isPresent()) {
            TableHandle target = analysis.getInsert().get().getTarget();
            connectors.add(target.getConnectorId());
        }

        return connectors.build();
    }

    @Override
    public String getSlug()
    {
        return slug;
    }

    @Override
    public int getRetryCount()
    {
        return retryCount;
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        SqlQuerySchedulerInterface scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getUserMemoryReservation();
        }
        if (scheduler == null) {
            return new DataSize(0, BYTE);
        }
        return succinctBytes(scheduler.getUserMemoryReservation());
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        SqlQuerySchedulerInterface scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalMemoryReservation();
        }
        if (scheduler == null) {
            return new DataSize(0, BYTE);
        }
        return succinctBytes(scheduler.getTotalMemoryReservation());
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        SqlQuerySchedulerInterface scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalCpuTime();
        }
        if (scheduler == null) {
            return new Duration(0, SECONDS);
        }
        return scheduler.getTotalCpuTime();
    }

    @Override
    public DataSize getRawInputDataSize()
    {
        SqlQuerySchedulerInterface scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getRawInputDataSize();
        }
        if (scheduler == null) {
            return new DataSize(0, BYTE);
        }
        return scheduler.getRawInputDataSize();
    }

    @Override
    public DataSize getOutputDataSize()
    {
        SqlQuerySchedulerInterface scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getOutputDataSize();
        }
        if (scheduler == null) {
            return new DataSize(0, BYTE);
        }
        return scheduler.getOutputDataSize();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getFinalQueryInfo()
                .map(BasicQueryInfo::new)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.ofNullable(queryScheduler.get()).map(SqlQuerySchedulerInterface::getBasicStageStats)));
    }

    @Override
    public int getRunningTaskCount()
    {
        return stateMachine.getCurrentRunningTaskCount();
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            try {
                // transition to planning
                if (!stateMachine.transitionToPlanning()) {
                    // query already started or finished
                    return;
                }

                PlanRoot plan;

                // set a thread timeout in case query analyzer ends up in an infinite loop
                try (TimeoutThread unused = new TimeoutThread(
                        Thread.currentThread(),
                        timeoutThreadExecutor,
                        getQueryAnalyzerTimeout(getSession()))) {
                    // analyze query
                    plan = analyzeQuery();
                }

                if (plan == null) {
                    stateMachine.transitionToFinishing();
                    return;
                }
                // todo catch if plan is null -> then do not execute it
                metadata.beginQuery(getSession(), plan.getConnectors());

                // plan distribution of query
                planDistribution(plan);

                // transition to starting
                if (!stateMachine.transitionToStarting()) {
                    // query already started or finished
                    return;
                }

                // if query is not finished, start the scheduler, otherwise cancel it
                SqlQuerySchedulerInterface scheduler = queryScheduler.get();

                if (!stateMachine.isDone()) {
                    scheduler.start();
                }
            }
            catch (Throwable e) {
                fail(e);
                throwIfInstanceOf(e, Error.class);
            }
        }
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            stateMachine.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public Optional<ResourceGroupQueryLimits> getResourceGroupQueryLimits()
    {
        return resourceGroupQueryLimits.get();
    }

    @Override
    public void setResourceGroupQueryLimits(ResourceGroupQueryLimits resourceGroupQueryLimits)
    {
        if (!this.resourceGroupQueryLimits.compareAndSet(Optional.empty(), Optional.of(requireNonNull(resourceGroupQueryLimits, "resourceGroupQueryLimits is null")))) {
            throw new IllegalStateException("Cannot set resourceGroupQueryLimits more than once");
        }
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    public BaoConnector getBaoConnector()
    {
        return baoConnector;
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    private PlanRoot analyzeQuery()
    {
        try {
            return doAnalyzeQuery();
        }
        catch (StackOverflowError e) {
            throw new PrestoException(NOT_SUPPORTED, "statement is too large (stack overflow during analysis)", e);
        }
    }

    private PlanRoot doAnalyzeQuery()
    {
        // time analysis phase
        stateMachine.beginAnalysis();

        Plan plan;

        // plan query
        // *** Bao Integration
        if (SystemSessionProperties.isGetQuerySpan(getSession())) {
            OptimizerConfiguration optimizerConfiguration = getSession().getOptimizerConfiguration();
            optimizerConfiguration.reset();

            Set<EffectiveOptimizerPart> requiredOptimizers = new HashSet<>();

            // 1. get default effective rules and optimizers (stored in OptimizerConfig)
            LogicalPlanner logicalPlanner = new LogicalPlanner(false, stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser, statsCalculator, costCalculator, stateMachine.getWarningCollector(), planChecker);
            plan = getSession().getRuntimeStats().profileNanos(
                    LOGICAL_PLANNER_TIME_NANOS,
                    () -> logicalPlanner.plan(analysis));
            queryPlan.set(plan);

            // initialize the optimizer span with effective default optimizers, used to detect required optimizers
            QuerySpan querySpan = new QuerySpan(optimizerConfiguration.getEffectiveOptimizers());
            while (querySpan.hasNext()) {
                // enable all optimizers and rules again and find alternative optimizers
                optimizerConfiguration.reset();

                // disable effective rule or optimizer and track new effective optimizers
                EffectiveOptimizerPart optimizerPart = querySpan.next();
                optimizerPart.disableOptimizerAndDependencies(optimizerConfiguration);

                Set<EffectiveOptimizerPart> disabledOptimizers = new HashSet<>();
                disabledOptimizers.add(optimizerPart);
                detectHitsInLogicalOptimization(requiredOptimizers, querySpan, disabledOptimizers);
            }

            // we know the required optimizers now, remove the found alternative optimizers and run approximation again
            List<EffectiveOptimizerPart> alternativeOptimizers = new ArrayList<>();
            for (EffectiveOptimizerPart eop : querySpan.getSeenOptimizers()) {
                if (!eop.isDefaultOptimizer()) {
                    alternativeOptimizers.add(eop);
                }
            }
            for (EffectiveOptimizerPart eop : alternativeOptimizers) {
                querySpan.getSeenOptimizers().remove(eop);
            }

            int numRequiredOptimizers = requiredOptimizers.size();
            int numNewEffectiveOptimizers = querySpan.getSeenOptimizers().size();
            Set<EffectiveOptimizerPart> knownOptimizers = new HashSet<>(querySpan.getSeenOptimizers());
            while (numNewEffectiveOptimizers > 0) {
                // keep track of effective optimizers
                Set<EffectiveOptimizerPart> effectiveOptimizers = new HashSet<>(querySpan.getSeenOptimizers());

                // enable all optimizers and rules again and find alternative optimizers
                optimizerConfiguration.reset();

                // disable effective rule or optimizer and track new effective optimizers
                for (EffectiveOptimizerPart eop : querySpan.getSeenOptimizers()) {
                    if (requiredOptimizers.contains(eop)) {
                        continue;
                    }
                    eop.disableOptimizerAndDependencies(optimizerConfiguration);
                }

                detectHitsInLogicalOptimization(requiredOptimizers, querySpan, effectiveOptimizers);
                if (numRequiredOptimizers < requiredOptimizers.size()) {
                    // we detected new required optimizers, re-run the detection of alternative rules
                    numRequiredOptimizers = requiredOptimizers.size();
                    continue;
                }
                // find new alternative rules
                Set<EffectiveOptimizerPart> newAlternativeRules = new HashSet<>(querySpan.getSeenOptimizers().stream().filter((EffectiveOptimizerPart e) -> !e.getOptimizerDependencies().isEmpty()).collect(Collectors.toList()));
                newAlternativeRules.removeAll(knownOptimizers);
                knownOptimizers.addAll(newAlternativeRules);
                numNewEffectiveOptimizers = newAlternativeRules.size();
            }

            // send query span to driver
            baoConnector.exportEffectiveOptimizerParts(querySpan.getSeenOptimizers());
            baoConnector.exportRequiredOptimizerParts(requiredOptimizers);

            optimizerConfiguration.reset();
            return null;
        }
        // Regular query execution
        LogicalPlanner logicalPlanner = new LogicalPlanner(false, stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser, statsCalculator, costCalculator, stateMachine.getWarningCollector(), planChecker);
        plan = getSession().getRuntimeStats().profileNanos(
                LOGICAL_PLANNER_TIME_NANOS,
                () -> logicalPlanner.plan(analysis));
        queryPlan.set(plan);

        // *** Bao integration
        // export logical graphviz plan
        Session session = getSession();
        Map<String, String> config = session.getSystemProperties();
        if (isExportGraphviz(getSession())) {
            String logicalGraphivz = PlanPrinter.graphvizLogicalPlan(plan.getRoot(), plan.getTypes(), stateMachine.getSession(), metadata.getFunctionAndTypeManager());
            baoConnector.exportGraphivzPlan("logical:" + logicalGraphivz);
        }
        // ***

        // extract inputs
        List<Input> inputs = new InputExtractor(metadata, stateMachine.getSession()).extractInputs(plan.getRoot());
        stateMachine.setInputs(inputs);

        // extract output
        Optional<Output> output = new OutputExtractor().extractOutput(plan.getRoot());
        stateMachine.setOutput(output);

        // fragment the plan
        // the variableAllocator is finally passed to SqlQueryScheduler for runtime cost-based optimizations
        variableAllocator.set(new PlanVariableAllocator(plan.getTypes().allVariables()));
        SubPlan fragmentedPlan = getSession().getRuntimeStats().profileNanos(
                FRAGMENT_PLAN_TIME_NANOS,
                () -> planFragmenter.createSubPlans(stateMachine.getSession(), plan, false, idAllocator, variableAllocator.get(), stateMachine.getWarningCollector()));

        // record analysis time
        stateMachine.endAnalysis();

        // keep the hash of the final plan
        String json = PlanPrinter.jsonLogicalPlan(plan.getRoot(), plan.getTypes(), metadata.getFunctionAndTypeManager(), plan.getStatsAndCosts(), stateMachine.getSession());
        getSession().getOptimizerConfiguration().planHash = json.hashCode();

        // export fragmented graphviz plan
        if (isExportGraphviz(getSession())) {
            String fragmentedGraphviz = PlanPrinter.graphvizDistributedPlan(fragmentedPlan, stateMachine.getSession(), metadata.getFunctionAndTypeManager());
            baoConnector.exportGraphivzPlan("fragmented:" + fragmentedGraphviz);
        }

        // export logical graphviz plan
        if (isExportJSON(getSession())) {
            baoConnector.exportJsonPlan("logical:" + json);

            String fragmentedJson = PlanPrinter.jsonFragmentPlan(fragmentedPlan.getFragment().getRoot(), fragmentedPlan.getFragment().getVariables(), metadata.getFunctionAndTypeManager(), getSession());
            baoConnector.exportJsonPlan("fragmented:" + fragmentedJson);
        }

        boolean explainAnalyze = analysis.getStatement() instanceof Explain && ((Explain) analysis.getStatement()).isAnalyze();

        if (!isExecuteQuery(getSession())) {
            return null;
        }

        return new PlanRoot(fragmentedPlan, !explainAnalyze, extractConnectors(analysis));
    }

    // todo: disable multiple optimizers at once here to find alternative rules iteratively?
    private void detectHitsInLogicalOptimization(Set<EffectiveOptimizerPart> requiredOptimizersOrRules, QuerySpan querySpan, Set<EffectiveOptimizerPart> disabledOptimizer)
    {
        OptimizerConfiguration optimizerConfiguration = getSession().getOptimizerConfiguration();
        LogicalPlanner logicalPlanner;
        Plan plan;
        logicalPlanner = new LogicalPlanner(false, stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser, statsCalculator, costCalculator, stateMachine.getWarningCollector(), planChecker);
        try {
            plan = getSession().getRuntimeStats().profileNanos(
                    LOGICAL_PLANNER_TIME_NANOS,
                    () -> logicalPlanner.plan(analysis));
            queryPlan.set(plan);

            // track effective queries here in case the query plan is valid
            HashSet<EffectiveOptimizerPart> effectiveRulesAndOptimizers = new HashSet<>(optimizerConfiguration.getEffectiveOptimizers());
            querySpan.addAlternativeRulesAndOptimizers(ImmutableSet.copyOf(disabledOptimizer), effectiveRulesAndOptimizers);
        }
        catch (Exception e) {
            requiredOptimizersOrRules.addAll(disabledOptimizer);
        }
    }

    private void planDistribution(PlanRoot plan)
    {
        CloseableSplitSourceProvider splitSourceProvider = new CloseableSplitSourceProvider(splitManager::getSplits);

        // ensure split sources are closed
        stateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                splitSourceProvider.close();
            }
        });

        // if query was canceled, skip creating scheduler
        if (stateMachine.isDone()) {
            return;
        }

        SubPlan outputStagePlan = plan.getRoot();

        // record output field
        stateMachine.setColumns(((OutputNode) outputStagePlan.getFragment().getRoot()).getColumnNames(), outputStagePlan.getFragment().getTypes());

        PartitioningHandle partitioningHandle = outputStagePlan.getFragment().getPartitioningScheme().getPartitioning().getHandle();
        OutputBuffers rootOutputBuffers;
        if (isSpoolingOutputBufferEnabled(getSession())) {
            rootOutputBuffers = createSpoolingOutputBuffers();
        }
        else {
            rootOutputBuffers = createInitialEmptyOutputBuffers(partitioningHandle)
                    .withBuffer(OUTPUT_BUFFER_ID, BROADCAST_PARTITION_ID)
                    .withNoMoreBufferIds();
        }

        SplitSourceFactory splitSourceFactory = new SplitSourceFactory(splitSourceProvider, stateMachine.getWarningCollector());
        // build the stage execution objects (this doesn't schedule execution)
        SqlQuerySchedulerInterface scheduler = isUseLegacyScheduler(getSession()) ?
                LegacySqlQueryScheduler.createSqlQueryScheduler(
                        locationFactory,
                        executionPolicy,
                        queryExecutor,
                        schedulerStats,
                        sectionExecutionFactory,
                        remoteTaskFactory,
                        splitSourceFactory,
                        stateMachine.getSession(),
                        metadata.getFunctionAndTypeManager(),
                        stateMachine,
                        outputStagePlan,
                        rootOutputBuffers,
                        plan.isSummarizeTaskInfos(),
                        runtimePlanOptimizers,
                        stateMachine.getWarningCollector(),
                        idAllocator,
                        variableAllocator.get(),
                        planChecker,
                        metadata,
                        sqlParser,
                        partialResultQueryManager) :
                SqlQueryScheduler.createSqlQueryScheduler(
                        locationFactory,
                        executionPolicy,
                        queryExecutor,
                        schedulerStats,
                        sectionExecutionFactory,
                        remoteTaskFactory,
                        splitSourceFactory,
                        internalNodeManager,
                        stateMachine.getSession(),
                        stateMachine,
                        outputStagePlan,
                        plan.isSummarizeTaskInfos(),
                        metadata.getFunctionAndTypeManager(),
                        runtimePlanOptimizers,
                        stateMachine.getWarningCollector(),
                        idAllocator,
                        variableAllocator.get(),
                        planChecker,
                        metadata,
                        sqlParser,
                        partialResultQueryManager);

        queryScheduler.set(scheduler);

        // if query was canceled during scheduler creation, abort the scheduler
        // directly since the callback may have already fired
        if (stateMachine.isDone()) {
            scheduler.abort();
            queryScheduler.set(null);
        }
    }

    @Override
    public void cancelQuery()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            SqlQuerySchedulerInterface scheduler = queryScheduler.get();
            if (scheduler != null) {
                scheduler.cancelStage(stageId);
            }
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        stateMachine.transitionToFailed(cause);

        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        SqlQuerySchedulerInterface scheduler = queryScheduler.get();
        stateMachine.updateQueryInfo(Optional.ofNullable(scheduler).map(SqlQuerySchedulerInterface::getStageInfo));
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        stateMachine.addOutputInfoListener(listener);
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        stateMachine.pruneQueryInfo();
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            // acquire reference to scheduler before checking finalQueryInfo, because
            // state change listener sets finalQueryInfo and then clears scheduler when
            // the query finishes.
            SqlQuerySchedulerInterface scheduler = queryScheduler.get();

            return stateMachine.getFinalQueryInfo().orElseGet(() -> buildQueryInfo(scheduler));
        }
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Plan getQueryPlan()
    {
        return queryPlan.get();
    }

    private QueryInfo buildQueryInfo(SqlQuerySchedulerInterface scheduler)
    {
        Optional<StageInfo> stageInfo = Optional.empty();
        if (scheduler != null) {
            stageInfo = Optional.of(scheduler.getStageInfo());
        }

        QueryInfo queryInfo = stateMachine.updateQueryInfo(stageInfo);
        if (queryInfo.isFinalQueryInfo()) {
            // capture the final query state and drop reference to the scheduler
            queryScheduler.set(null);
        }

        return queryInfo;
    }

    private static class PlanRoot
    {
        private final SubPlan root;
        private final boolean summarizeTaskInfos;
        private final Set<ConnectorId> connectors;

        public PlanRoot(SubPlan root, boolean summarizeTaskInfos, Set<ConnectorId> connectors)
        {
            this.root = requireNonNull(root, "root is null");
            this.summarizeTaskInfos = summarizeTaskInfos;
            this.connectors = ImmutableSet.copyOf(connectors);
        }

        public SubPlan getRoot()
        {
            return root;
        }

        public boolean isSummarizeTaskInfos()
        {
            return summarizeTaskInfos;
        }

        public Set<ConnectorId> getConnectors()
        {
            return connectors;
        }
    }

    public static class SqlQueryExecutionFactory
            implements QueryExecutionFactory<QueryExecution>
    {
        private final SplitSchedulerStats schedulerStats;
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final SqlParser sqlParser;
        private final SplitManager splitManager;
        private final List<PlanOptimizer> planOptimizers;
        private final List<PlanOptimizer> runtimePlanOptimizers;
        private final PlanFragmenter planFragmenter;
        private final RemoteTaskFactory remoteTaskFactory;
        private final QueryExplainer queryExplainer;
        private final LocationFactory locationFactory;
        private final ScheduledExecutorService timeoutThreadExecutor;
        private final ExecutorService queryExecutor;
        private final SectionExecutionFactory sectionExecutionFactory;
        private final InternalNodeManager internalNodeManager;
        private final Map<String, ExecutionPolicy> executionPolicies;
        private final StatsCalculator statsCalculator;
        private final CostCalculator costCalculator;
        private final PlanChecker planChecker;
        private final PartialResultQueryManager partialResultQueryManager;

        @Inject
        SqlQueryExecutionFactory(QueryManagerConfig config,
                Metadata metadata,
                AccessControl accessControl,
                SqlParser sqlParser,
                LocationFactory locationFactory,
                SplitManager splitManager,
                PlanOptimizers planOptimizers,
                PlanFragmenter planFragmenter,
                RemoteTaskFactory remoteTaskFactory,
                @ForQueryExecution ExecutorService queryExecutor,
                @ForTimeoutThread ScheduledExecutorService timeoutThreadExecutor,
                SectionExecutionFactory sectionExecutionFactory,
                InternalNodeManager internalNodeManager,
                QueryExplainer queryExplainer,
                Map<String, ExecutionPolicy> executionPolicies,
                SplitSchedulerStats schedulerStats,
                StatsCalculator statsCalculator,
                CostCalculator costCalculator,
                PlanChecker planChecker,
                PartialResultQueryManager partialResultQueryManager)
        {
            requireNonNull(config, "config is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            requireNonNull(planOptimizers, "planOptimizers is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.timeoutThreadExecutor = requireNonNull(timeoutThreadExecutor, "timeoutThreadExecutor is null");
            this.sectionExecutionFactory = requireNonNull(sectionExecutionFactory, "sectionExecutionFactory is null");
            this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.executionPolicies = requireNonNull(executionPolicies, "schedulerPolicies is null");
            this.planOptimizers = planOptimizers.getPlanningTimeOptimizers();
            this.runtimePlanOptimizers = planOptimizers.getRuntimeOptimizers();
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.planChecker = requireNonNull(planChecker, "planChecker is null");
            this.partialResultQueryManager = requireNonNull(partialResultQueryManager, "partialResultQueryManager is null");
        }

        @Override
        public QueryExecution createQueryExecution(
                PreparedQuery preparedQuery,
                QueryStateMachine stateMachine,
                String slug,
                int retryCount,
                WarningCollector warningCollector,
                Optional<QueryType> queryType)
        {
            String executionPolicyName = getExecutionPolicy(stateMachine.getSession());
            ExecutionPolicy executionPolicy = executionPolicies.get(executionPolicyName);
            checkArgument(executionPolicy != null, "No execution policy %s", executionPolicy);

            SqlQueryExecution execution = new SqlQueryExecution(
                    preparedQuery,
                    stateMachine,
                    slug,
                    retryCount,
                    metadata,
                    accessControl,
                    sqlParser,
                    splitManager,
                    planOptimizers,
                    runtimePlanOptimizers,
                    planFragmenter,
                    remoteTaskFactory,
                    locationFactory,
                    queryExecutor,
                    timeoutThreadExecutor,
                    sectionExecutionFactory,
                    internalNodeManager,
                    queryExplainer,
                    executionPolicy,
                    schedulerStats,
                    statsCalculator,
                    costCalculator,
                    warningCollector,
                    planChecker,
                    partialResultQueryManager);

            return execution;
        }
    }
}
