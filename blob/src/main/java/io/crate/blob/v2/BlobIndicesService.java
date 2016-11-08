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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.FutureActionListener;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;

import java.util.HashMap;

public class BlobIndicesService extends AbstractLifecycleComponent implements IndexEventListener {

    public static final Setting<String> SETTING_BLOBS_PATH = Setting.simpleString("blobs.path", Setting.Property.NodeScope);
    public static final Setting<Boolean> SETTING_INDEX_BLOBS_ENABLED = Setting.boolSetting("index.blobs.enabled", false, Setting.Property.IndexScope);
    public static final Setting<String> SETTING_INDEX_BLOBS_PATH = Setting.simpleString("index.blobs.path", Setting.Property.IndexScope);

    private static final String INDEX_PREFIX = ".blob_";

    private final ClusterService clusterService;

    private final Client client;
    private final HashMap<String, BlobIndex> indices = new HashMap<>();


    public static final Predicate<String> indicesFilter = new Predicate<String>() {
        @Override
        public boolean apply(String indexName) {
            return isBlobIndex(indexName);
        }
    };

    public static final Function<String, String> STRIP_PREFIX = new Function<String, String>() {
        @Override
        public String apply(String indexName) {
            return indexName(indexName);
        }
    };

    @Inject
    public BlobIndicesService(Client client,
                              ClusterService clusterService) {
        super(clusterService.getSettings());
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doStart() {
        // XDOBE: check if we need to be a lifecyclecomp
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    public BlobShard blobShardSafe(ShardId shardId) {
        BlobShard bs = blobShard(shardId);
        if (bs != null) {
            return bs;
        } else {
            IndexMetaData indexMetaData = clusterService.state().metaData().index(shardId.getIndex());
            if (indexMetaData == null) {
                throw new IndexNotFoundException(shardId.getIndexName());
            }
            if (!BlobIndicesService.SETTING_INDEX_BLOBS_ENABLED.get(indexMetaData.getSettings())) {
                throw new BlobsDisabledException(shardId.getIndex());
            }
            throw new ShardNotFoundException(shardId);
        }
    }

    /**
     * can be used to alter the number of replicas.
     *
     * @param tableName     name of the blob table
     * @param indexSettings updated index settings
     */
    public ListenableFuture<Void> alterBlobTable(String tableName, Settings indexSettings) {
        FutureActionListener<UpdateSettingsResponse, Void> listener =
            new FutureActionListener<>(Functions.<Void>constant(null));

        client.admin().indices().updateSettings(
            new UpdateSettingsRequest(indexSettings, fullIndexName(tableName)), listener);
        return listener;
    }

    public ListenableFuture<Void> createBlobTable(String tableName,
                                                  Settings indexSettings) {
        Settings.Builder builder = Settings.builder();
        builder.put(indexSettings);
        builder.put(SETTING_INDEX_BLOBS_ENABLED.getKey(), true);

        final SettableFuture<Void> result = SettableFuture.create();
        client.admin().indices().create(new CreateIndexRequest(fullIndexName(tableName), builder.build()), new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                assert createIndexResponse.isAcknowledged();
                result.set(null);
            }

            @Override
            public void onFailure(Exception e) {
                result.setException(e);
            }
        });
        return result;
    }

    public ListenableFuture<Void> dropBlobTable(final String tableName) {
        FutureActionListener<DeleteIndexResponse, Void> listener = new FutureActionListener<>(Functions.<Void>constant(null));
        client.admin().indices().delete(new DeleteIndexRequest(fullIndexName(tableName)), listener);
        return listener;
    }

    public BlobShard blobShard(ShardId shardId) {
        return indices.get(shardId.getIndex().getUUID()).getBlobShard(shardId.id());
    }

    /**
     * Returns the local shardId of the given blob
     *
     * @return the local shardId
     * @throws IndexNotFoundException if provided index does not exist
     * @throws ShardNotFoundException if the computed shard id is not available locally
     */
    private ShardId localShardId(String indexName, String digest) {
        return clusterService.operationRouting().getShards(clusterService.state(), indexName, null, digest, "_only_local").shardId();
    }

//    public Index getIndex(String index){
//        assert isBlobIndex(index): "only blob indices are allowed";
//        IndexMetaData indexMetaData = clusterService.state().metaData().index(index);
//        if (indexMetaData != null){
//            if (indexMetaData.getIndex().)
//            return indexMetaData.getIndex();
//        }
//        assert indexMetaData != null;
//
//        retu
//        if (indexMetaData == null) {
//            throw new IndexNotFoundException(index);
//        }
//        return indexMetaData.getIndex();
//    }

    /**
     * Returns the local BlobShard of the given blob
     * @return the local shardId
     * @throws IndexNotFoundException if provided index does not exist
     * @throws ShardNotFoundException if the computed shard id is not available locally
     */
    public BlobShard localBlobShard(String index, String digest) {
        ShardId shardId = localShardId(index, digest);
        return blobShardSafe(shardId);
    }


    public BlobShard localBlobShard(Index index, String digest) {
        return blobShardSafe(localShardId(index.getName(), digest));
    }


//    public BlobShardFuture blobShardFuture(String index, int shardId) {
//        return new BlobShardFuture(this, indicesLifecycle, index, shardId);
//
//    }

    /**
     * check if this index is a blob table
     * <p>
     * This only works for indices that were created via SQL.
     */
    public static boolean isBlobIndex(String indexName) {
        return indexName.startsWith(INDEX_PREFIX);
    }

    /**
     * check if given shard is part of an index that is a blob table
     * <p>
     * This only works for indices that were created via SQL.
     */
    public static boolean isBlobShard(ShardId shardId) {
        return isBlobIndex(shardId.getIndexName());
    }

    /**
     * Returns the full index name, adds blob index prefix.
     */
    public static String fullIndexName(String indexName) {
        if (isBlobIndex(indexName)) {
            return indexName;
        }
        return INDEX_PREFIX + indexName;
    }

    /**
     * Strips the blob index prefix from a full index name
     */
    public static String indexName(String indexName) {
        if (!isBlobIndex(indexName)) {
            return indexName;
        }
        return indexName.substring(INDEX_PREFIX.length());
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        // called when the index gets created on a data node
        logger.debug("afterIndexCreated {}", indexService.index());
        assert BlobIndicesService.SETTING_INDEX_BLOBS_ENABLED.get(indexService.getMetaData().getSettings()) :
            "blobs need to be enabled on index";
        BlobIndex blobIndex = new BlobIndex(indexService);
        assert !indices.containsKey(indexService.indexUUID()) : "blob index already exists";
        indices.put(indexService.indexUUID(), blobIndex);
    }

    @Override
    public void beforeIndexClosed(IndexService indexService) {
        logger.debug("beforeIndexClosed {}", indexService.index());
        assert BlobIndicesService.SETTING_INDEX_BLOBS_ENABLED.get(indexService.getMetaData().getSettings()) :
            "blobs need to be enabled on index";
        assert indices.containsKey(indexService.indexUUID()) : "blob index does not exist upon close";
        indices.remove(indexService.indexUUID());
    }

    @Override
    public void afterIndexDeleted(Index index, Settings indexSettings) {
        logger.debug("afterIndexDeleted {}", index);
        assert BlobIndicesService.SETTING_INDEX_BLOBS_ENABLED.get(indexSettings) : "blobs need to be enabled on index";
        BlobIndex blobIndex = indices.remove(index.getUUID());
        assert blobIndex != null : "blob index not found upon index deletion";
        assert blobIndex.isEmpty() : "blob index still has shards";
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        BlobIndex blobIndex = getBlobIndex(indexShard.shardId().getIndex().getUUID());
        blobIndex.newShard(indexShard);
    }

    private BlobIndex getBlobIndex(String uuid){
        BlobIndex blobIndex = indices.get(uuid);
        assert blobIndex != null : "blob index not found";
        return blobIndex;
    }

    private BlobShard getBlobShard(ShardId shardId){
        BlobShard bs = getBlobIndex(shardId.getIndex().getUUID()).getBlobShard(shardId.getId());
        assert bs != null: "blob shard not found";
        return bs;
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        logger.debug("afterShardClosed {}", shardId);
        BlobShard blobShard = getBlobShard(shardId);
    }

    @Override
    public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        BlobIndex blobIndex = getBlobIndex(shardId.getIndex().getUUID());
        BlobShard bs = blobIndex.removeShard(shardId.getId());
        assert bs != null: "blob shard not found";
        bs.deletePath();
    }
}
