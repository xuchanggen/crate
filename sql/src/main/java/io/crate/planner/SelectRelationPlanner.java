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

package io.crate.planner;

import com.google.common.base.Optional;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.metadata.RowGranularity;
import io.crate.operation.projectors.TopN;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiFunction;

class SelectRelationPlanner {

    private static class FieldReplacer extends ReplacingSymbolVisitor<List<Field>> {

        private final static FieldReplacer INSTANCE = new FieldReplacer();

        FieldReplacer() {
            super(ReplaceMode.MUTATE);
        }

        @Override
        public Symbol visitField(Field field, List<Field> subRelationFields) {
            ListIterator<Field> it = subRelationFields.listIterator();
            while (it.hasNext()) {
                Field subRelationField = it.next();
                if (subRelationField.path().outputName().equals(field.path().outputName())) {
                    return new InputColumn(it.previousIndex(), field.valueType());
                }
            }
            return field;
        }
    }

    public static Plan plan(Functions functions,
                            QueriedSelectRelation relation,
                            Planner.Context context,
                            BiFunction<AnalyzedRelation, Planner.Context, Plan> consumingPlanner) {
        QueriedRelation subRelation = relation.subRelation();
        Plan subPlan = consumingPlanner.apply(subRelation, context);
        QuerySpec querySpec = relation.querySpec();
        // TODO: for some reason there are only fields here if in the subquery there is a unnest function
        // fix this
        querySpec.replace(x -> FieldReplacer.INSTANCE.process(x, subRelation.fields()));
        ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions, querySpec);

        List<Projection> mergeProjections = new ArrayList<>();
        List<Symbol> topNInputs = querySpec.outputs();
        if (querySpec.hasAggregates()) {
            SplitPoints splitPoints = projectionBuilder.getSplitPoints();
            Projection partialAggregation = createPartialAggregation(projectionBuilder, splitPoints);
            subPlan.addProjection(partialAggregation, null, null, partialAggregation.outputs().size(), null);
            mergeProjections.add(createFinalAggregation(projectionBuilder, splitPoints));

            if (querySpec.groupBy().isPresent()) {
                topNInputs = new ArrayList<>();
                topNInputs.addAll(splitPoints.toCollect());
                topNInputs.addAll(splitPoints.aggregates());
            }
        }
        subPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
        List<String> handlerNodes = Collections.singletonList(context.handlerNode());
        if (subPlan instanceof Merge) {
            ((Merge) subPlan).mergePhase().executionNodes(handlerNodes);
        }
        Limits limits = context.getLimits(querySpec);
        Optional<OrderBy> optOrderBy = querySpec.orderBy();
        // always add topN even if there is no limit, offset or orderBy to re-arrange columns or strip unnecessary outputs
        // that may be there because of a GroupProjection
        TopNProjection topNProjection = ProjectionBuilder.topNProjection(
            topNInputs,
            optOrderBy.orNull(),
            limits.offset(),
            limits.finalLimit(),
            querySpec.outputs()
        );
        mergeProjections.add(topNProjection);
        Merge merge = Merge.create(
            subPlan,
            context,
            mergeProjections,
            TopN.NO_LIMIT,
            0,
            mergeProjections.get(mergeProjections.size() - 1).outputs().size()
        );
        merge.mergePhase().executionNodes(handlerNodes);
        return merge;
    }

    private static Projection createFinalAggregation(ProjectionBuilder projectionBuilder, SplitPoints splitPoints) {
        if (splitPoints.querySpec().groupBy().isPresent()) {
            List<Symbol> prevGroupByOutputs =
                new ArrayList<>(splitPoints.toCollect().size() + splitPoints.aggregates().size());
            prevGroupByOutputs.addAll(splitPoints.toCollect());
            prevGroupByOutputs.addAll(splitPoints.aggregates());
            return projectionBuilder.groupProjection(
                prevGroupByOutputs,
                splitPoints.toCollect(),
                splitPoints.aggregates(),
                Aggregation.Step.PARTIAL,
                Aggregation.Step.FINAL,
                RowGranularity.CLUSTER
            );
        }
        return projectionBuilder.aggregationProjection(
            splitPoints.aggregates(),
            splitPoints.aggregates(),
            Aggregation.Step.PARTIAL,
            Aggregation.Step.FINAL,
            RowGranularity.CLUSTER
        );
    }

    private static Projection createPartialAggregation(ProjectionBuilder projectionBuilder, SplitPoints splitPoints) {
        if (splitPoints.querySpec().groupBy().isPresent()) {
            return projectionBuilder.groupProjection(
                splitPoints.leaves(),
                splitPoints.toCollect(),
                splitPoints.aggregates(),
                Aggregation.Step.ITER,
                Aggregation.Step.PARTIAL,
                RowGranularity.CLUSTER
            );
        }
        return projectionBuilder.aggregationProjection(
            splitPoints.leaves(),
            splitPoints.aggregates(),
            Aggregation.Step.ITER,
            Aggregation.Step.PARTIAL,
            RowGranularity.CLUSTER
        );
    }
}
