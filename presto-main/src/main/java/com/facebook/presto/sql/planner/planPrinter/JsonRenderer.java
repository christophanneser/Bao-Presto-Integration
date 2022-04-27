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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonRenderer
        implements Renderer<String>
{
    private static final JsonCodec<JsonRenderedNode> CODEC = JsonCodec.jsonCodec(JsonRenderedNode.class);
    private static final JsonCodec<Map<PlanFragmentId, JsonPlanFragment>> PLAN_MAP_CODEC = JsonCodec.mapJsonCodec(PlanFragmentId.class, JsonPlanFragment.class);

    private static String formatEstimateAsDataSize(double value)
    {
        return isNaN(value) ? "'NA'" : succinctBytes((long) value).toString();
    }

    private static String formatAsLong(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%d", Math.round(value));
        }

        return "'NA'";
    }

    static String formatDouble(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%.2f", value);
        }

        return "'NA'";
    }

    static String formatPositions(long positions)
    {
        String noun = (positions == 1) ? "row" : "rows";
        return positions + " " + noun;
    }

    @Override
    public String render(PlanRepresentation plan)
    {
        return CODEC.toJson(renderJson(plan, plan.getRoot()));
    }

    public String render(Map<PlanFragmentId, JsonPlanFragment> fragmentJsonMap)
    {
        return PLAN_MAP_CODEC.toJson(fragmentJsonMap);
    }

    private JsonRenderedNode renderJson(PlanRepresentation plan, NodeRepresentation node)
    {
        List<JsonRenderedNode> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(n -> renderJson(plan, n))
                .collect(toImmutableList());

        return new JsonRenderedNode(
                node,
                node.getSourceLocation(),
                node.getId().toString(),
                node.getName(),
                node.getIdentifier(),
                node.getDetails(),
                children,
                node.getRemoteSources().stream()
                        .map(PlanFragmentId::toString)
                        .collect(toImmutableList()));
    }

    public static class JsonRenderedNode
    {
        private final NodeRepresentation node;
        private final Optional<SourceLocation> sourceLocation;
        private final String id;
        private final String name;
        private final String identifier;
        private final String details;
        private final List<JsonRenderedNode> children;
        private final List<String> remoteSources;
        private final Optional<String> tableName;

        @JsonCreator
        public JsonRenderedNode(NodeRepresentation node, Optional<SourceLocation> sourceLocation, String id, String name, String identifier, String details, List<JsonRenderedNode> children, List<String> remoteSources)
        {
            this.node = requireNonNull(node, "node is null");
            this.sourceLocation = sourceLocation;
            this.id = requireNonNull(id, "id is null");
            this.name = requireNonNull(name, "name is null");
            this.identifier = requireNonNull(identifier, "identifier is null");
            this.details = requireNonNull(details, "details is null");
            this.children = requireNonNull(children, "children is null");
            this.remoteSources = requireNonNull(remoteSources, "id is null");
            this.tableName = getOptionalTableName();
        }

        @JsonProperty
        public Optional<SourceLocation> getSourceLocation()
        {
            return sourceLocation;
        }

        @JsonProperty
        public String getId()
        {
            return id;
        }

        @JsonProperty
        public Optional<String> getTableName()
        {
            return tableName;
        }

        private Optional<String> getOptionalTableName()
        {
            Pattern tableNamePattern = Pattern.compile(".*tableName=(\\w*).*");
            Matcher matcher = tableNamePattern.matcher(identifier);
            if (matcher.matches()) {
                return Optional.of(matcher.group(1));
            }
            return Optional.empty();
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public String getIdentifier()
        {
            return identifier;
        }

        @JsonProperty
        public String getDetails()
        {
            return details;
        }

        @JsonProperty
        public List<JsonRenderedNode> getChildren()
        {
            return children;
        }

        @JsonProperty
        public List<String> getRemoteSources()
        {
            return remoteSources;
        }

        @JsonProperty
        public List<PlanCostEstimateWithRows> getEstimates()
        {
            List<PlanCostEstimateWithRows> estimates = new ArrayList<>();
            if (node.getEstimatedStats().stream().allMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown) &&
                    node.getEstimatedCost().stream().allMatch(c -> c.equals(PlanCostEstimate.unknown()))) {
                return estimates;
            }

            int estimateCount = node.getEstimatedStats().size();

            for (int i = 0; i < estimateCount; i++) {
                PlanNodeStatsEstimate stats = node.getEstimatedStats().get(i);
                PlanCostEstimate cost = node.getEstimatedCost().get(i);
                estimates.add(new PlanCostEstimateWithRows((long) stats.getOutputRowCount(),
                        (long) stats.getOutputSizeInBytes(node.getOutputs()), cost.getCpuCost(), cost.getMaxMemory(), cost.getMaxMemoryWhenOutputting(), cost.getNetworkCost()));
            }
            return estimates;
        }
    }

    public static class PlanCostEstimateWithRows
    {
        // todo check why some values are set to ZERO and how to represent NAN 'values'
        private final long rows;
        private final long rowsSize;
        private final double cpuCost;
        private final double maxMemory;
        private final double maxMemoryWhenOutputting;
        private final double networkCost;

        @JsonCreator
        public PlanCostEstimateWithRows(
                @JsonProperty("rows") long rows,
                @JsonProperty("rowsSize") long rowsSize,
                @JsonProperty("cpuCost") double cpuCost,
                @JsonProperty("maxMemory") double maxMemory,
                @JsonProperty("maxMemoryWhenOutputting") double maxMemoryWhenOutputting,
                @JsonProperty("networkCost") double networkCost)
        {
            checkArgument(!(rows < 0), "rows cannot be negative: %s", cpuCost);
            checkArgument(!(rowsSize < 0), "rowsSize cannot be negative: %s", cpuCost);
            checkArgument(!(cpuCost < 0), "cpuCost cannot be negative: %s", cpuCost);
            checkArgument(!(maxMemory < 0), "maxMemory cannot be negative: %s", maxMemory);
            checkArgument(!(maxMemoryWhenOutputting < 0), "maxMemoryWhenOutputting cannot be negative: %s", maxMemoryWhenOutputting);
            checkArgument(!(maxMemoryWhenOutputting > maxMemory), "maxMemoryWhenOutputting cannot be greater than maxMemory: %s > %s", maxMemoryWhenOutputting, maxMemory);
            checkArgument(!(networkCost < 0), "networkCost cannot be negative: %s", networkCost);
            this.rows = rows;
            this.rowsSize = rowsSize;
            this.cpuCost = cpuCost;
            this.maxMemory = maxMemory;
            this.maxMemoryWhenOutputting = maxMemoryWhenOutputting;
            this.networkCost = networkCost;
        }

        @JsonProperty
        public long getRows()
        {
            return rows;
        }

        @JsonProperty
        public long getRowsSize()
        {
            return rowsSize;
        }

        @JsonProperty
        public double getCpuCost()
        {
            return cpuCost;
        }

        @JsonProperty
        public double getMaxMemory()
        {
            return maxMemory;
        }

        @JsonProperty
        public double getMaxMemoryWhenOutputting()
        {
            return maxMemoryWhenOutputting;
        }

        @JsonProperty
        public double getNetworkCost()
        {
            return networkCost;
        }
    }

    public static class JsonPlanFragment
    {
        @JsonRawValue
        private final String plan;

        @JsonCreator
        public JsonPlanFragment(String plan)
        {
            this.plan = plan;
        }

        @JsonProperty
        public String getPlan()
        {
            return this.plan;
        }
    }
}
