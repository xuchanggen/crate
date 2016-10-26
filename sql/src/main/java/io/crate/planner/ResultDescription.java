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

import io.crate.planner.distribution.DistributionInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public interface ResultDescription {

    ResultDescription HANDLER_ROW_COUNT = new ResultDescription() {
        @Override
        public Collection<String> executionNodes() {
            return Collections.emptyList();
        }

        @Override
        public DistributionInfo distributionInfo() {
            return DistributionInfo.DEFAULT_BROADCAST;
        }

        @Override
        public void distributionInfo(DistributionInfo distributionInfo) {
            throw new UnsupportedOperationException("Cannot overwrite distributionInfo of HANLDER_ROW_COUNT ResultDescription");
        }

        @Override
        public List<DataType> streamedTypes() {
            return Collections.<DataType>singletonList(DataTypes.LONG);
        }

        @Nullable
        @Override
        public int[] orderByIndices() {
            return null;
        }

        @Nullable
        @Override
        public boolean[] reverseFlags() {
            return null;
        }

        @Nullable
        @Override
        public Boolean[] nullsFirst() {
            return null;
        }
    };

    Collection<String> executionNodes();

    DistributionInfo distributionInfo();

    void distributionInfo(DistributionInfo distributionInfo);

    List<DataType> streamedTypes();

    @Nullable int[] orderByIndices();

    @Nullable boolean[] reverseFlags();

    @Nullable Boolean[] nullsFirst();
}
