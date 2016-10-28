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


import io.crate.operation.Paging;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Plan;
import io.crate.planner.ResultDescription;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;

import java.util.Collections;

public class SimpleSelect {

    public static void enablePagingIfApplicable(Plan subPlan,
                                                MergePhase mergePhase,
                                                String localNodeId) {
        ResultDescription resultDescription = subPlan.resultDescription();
        if (!(subPlan instanceof Collect)) {
            return;
        }
        int limit = resultDescription.limit();
        int offset = resultDescription.offset();
        if (limit == TopN.NO_LIMIT || ((limit + offset) > Paging.PAGE_SIZE)) {
            mergePhase.executionNodes(Collections.singletonList(localNodeId));
            CollectPhase collectPhase = ((Collect) subPlan).collectPhase();
            if (collectPhase instanceof RoutedCollectPhase) {
                RoutedCollectPhase phase = (RoutedCollectPhase) collectPhase;
                if (phase.nodePageSizeHint() != null) {
                    // in the directResponse case nodePageSizeHint has probably be set to limit
                    // since it is now push based we can reduce the nodePageSizeHint
                    phase.pageSizeHint(limit + offset);
                }
            }
        }
    }
}
