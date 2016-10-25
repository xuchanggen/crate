/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.consumer;

import io.crate.analyze.QueriedTable;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.exceptions.VersionInvalidException;
import io.crate.planner.Plan;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.Collections;

/**
 * Consumer which can plan only simple selects that don't have any aggregations and doesn't have a order by or limit.
 *
 * <pre>
 *     select a, b from t
 * </pre>
 *
 * The result is
 *
 * {@link io.crate.planner.node.dql.Collect} - the result always being distributed
 * (unless there is only a single shard on the handler)
 */
public class CollectConsumer implements Consumer {

    private final Visitor visitor = new Visitor();

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    @Nullable
    private static Plan createPlan(QueriedTableRelation tableRelation, ConsumerContext consumerContext) {
        QuerySpec qs = tableRelation.querySpec();
        if (qs.groupBy().isPresent() || qs.hasAggregates() || qs.orderBy().isPresent() || qs.limit().isPresent()) {
            return null;
        }
        RoutedCollectPhase routedCollectPhase = RoutedCollectPhase.forQueriedTable(
            consumerContext.plannerContext(),
            tableRelation,
            qs.outputs(),
            Collections.<Projection>emptyList());
        return new Collect(routedCollectPhase);
    }

    private static class Visitor extends RelationPlanningVisitor {

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (table.querySpec().where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }
            return createPlan(table, context);
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, ConsumerContext context) {
            WhereClause where = table.querySpec().where();
            if (where.hasQuery()) {
                NoPredicateVisitor.ensureNoLuceneOnlyPredicates(where.query());
            }
            return createPlan(table, context);
        }
    }
}
