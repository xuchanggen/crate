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

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.operation.predicate.MatchPredicate;

class NoPredicateVisitor extends SymbolVisitor<Void, Void> {

    public static void ensureNoLuceneOnlyPredicates(Symbol query) {
        INSTANCE.process(query, null);
    }

    private static final NoPredicateVisitor INSTANCE = new NoPredicateVisitor();

    private NoPredicateVisitor() {
    }

    @Override
    public Void visitFunction(Function symbol, Void context) {
        if (symbol.info().ident().name().equals(MatchPredicate.NAME)) {
            throw new UnsupportedFeatureException("Cannot use match predicate on system tables");
        }
        for (Symbol argument : symbol.arguments()) {
            process(argument, context);
        }
        return null;
    }
}
