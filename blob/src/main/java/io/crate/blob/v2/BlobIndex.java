/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.blob.v2;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;

import java.nio.file.Path;

class BlobIndex extends AbstractIndexComponent {

    private final IntObjectMap<BlobShard> shards = new IntObjectHashMap<>();

    private final Path customPath;

    BlobIndex(IndexService indexService) {
        super(indexService.getIndexSettings());
        String pathFromSettings = BlobIndicesService.SETTING_INDEX_BLOBS_PATH.get(indexService.getMetaData().getSettings());
        if (pathFromSettings == null){
            pathFromSettings = BlobIndicesService.SETTING_BLOBS_PATH.get(indexService.getMetaData().getSettings());
        }
        if (pathFromSettings != null){
            customPath = PathUtils.get(pathFromSettings);
        } else {
            customPath = null;
        }
    }

    BlobShard getBlobShard(int id){
        return shards.get(id);
    }

    void newShard(IndexShard shard) {
        assert !shards.containsKey(shard.shardId().getId()): "shard already exists";
        if (shard.shardPath().isCustomDataPath()){
            throw new IllegalArgumentException("custom data paths are not supported for blob tables");
        }

        Path shardPath;
        if (customPath != null){
            Path relative = shard.shardPath().getRootStatePath().relativize(shard.shardPath().getShardStatePath());
            shardPath = customPath.resolve(relative);
        } else {
            shardPath = shard.shardPath().getDataPath();
        }
        BlobShard blobShard = new BlobShard(shard, shardPath);
        shards.put(shard.shardId().getId(), blobShard);
    }

    BlobShard removeShard(int id) {
        assert shards.containsKey(id): "shard does not exist";
        return shards.remove(id);
    }

    public boolean isEmpty() {
        return shards.isEmpty();
    }
}
