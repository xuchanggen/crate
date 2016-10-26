/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.consumer;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.planner.Plan;

/**
 * <p>
 * A consumer is a component which can create a Plan for a relation.
 * A consumer plans only so far that the result is complete, but still distributed if possible.
 *
 * </p>
 * For example:
 *
 * <pre>
 *     SELECT count(*), name from t group by 2
 *
 *                  |
 *
 *      NODE1      NODE2
 *       CP         CP
 *        |__      /|
 *        |  \____/ |
 *        |_/    \__|
 *       MP         MP      <--- Merge Phase / Reduce Phase
 *                               Result is "complete" here - so a consumer should stop planning here
 * </pre>
 *
 * Or:
 *
 * <pre>
 *     SELECT * from t        -- no limit
 *
 *     NODE1    NODE2
 *       CP       CP          <-- Result is complete
 *
 * BUT:
 *
 *     SELECT * from t limit 10
 *
 *     NODE1     NODE2
 *       CP        CP         <-- if CP includes limit 10 both nodes would result in 20 rows.
 *       |        /                 The result is incomplete
 *       |______/
 *       |
 *       MP (limit 10)        <-- Result is complete
 *                                (If it's on Node1 or Node2 doesn't matter)
 * </pre>
 */
public interface Consumer {

    Plan consume(AnalyzedRelation relation, ConsumerContext context);
}
