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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

public class TestBaoPipelines
        extends BasePlanTest
{
    @Test
    public void testOneRelation()
    {
        String statement = "SELECT * FROM lineitem";
        Plan plan = testPlanOptimization(statement);
        assert (plan.getBaoPipeline().getPipelineOptimizersConfigs().size() == 1);
    }

    @Test
    public void testTwoRelations()
    {
        String statement = "SELECT * FROM lineitem, orders limit 10";
        Plan plan = testPlanOptimization(statement);
        assert (plan.getBaoPipeline().getPipelineOptimizersConfigs().size() == 2);
    }

    @Test
    public void testThreeRelations()
    {
        String statement = "SELECT * FROM lineitem, orders, nation limit 10";
        Plan plan = testPlanOptimization(statement);
        assert (plan.getBaoPipeline().getPipelineOptimizersConfigs().size() == 3);
    }

    // todo add tests including aggregations, to see if those pipelines are also handled correctly
    @Test
    public void testAggregationPipeline()
    {
        String statement = "SELECT orders.orderdate FROM lineitem, orders, nation group by orders.orderdate limit 10";
        Plan plan = testPlanOptimization(statement);
        assert (plan.getBaoPipeline().getPipelineOptimizersConfigs().size() == 3);
    }
}
