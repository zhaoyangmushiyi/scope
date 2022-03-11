package com.supermap.services.tilesource.impl;

import com.supermap.services.components.commontypes.TileType;
import com.supermap.services.tilesource.ImageMetaData;
import com.supermap.services.tilesource.MetaData;
import com.supermap.services.tilesource.RealspaceImageMetaData;
import com.supermap.services.tilesource.ScopeTileSourceInfo;
import com.supermap.services.tilesource.TerrainMetaData;
import com.supermap.services.tilesource.TileSourceProviderAnnotation;
import com.supermap.services.tilesource.Tileset;
import io.transwarp.shiva2.client.RowResult;
import io.transwarp.shiva2.client.RowResultIterator;
import io.transwarp.shiva2.client.ShivaClient;
import io.transwarp.shiva2.client.ShivaScanToken;
import io.transwarp.shiva2.client.ShivaScanner;
import io.transwarp.shiva2.exception.ShivaException;
import io.transwarp.shiva2.metadata.ShivaTableMeta;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * 基于MongoDB存储的切片源
 * </p>
 * @author ${Author}
 * @version ${Version}
 * 
 */
@TileSourceProviderAnnotation(infoType = ScopeTileSourceInfo.class, name = "Scope", isLocal = false)
public class ScopeTileSourceProvider extends AbstractTileSourceProvider<ScopeTileSourceInfo> {

    private static final String DEFAULT_DBNAME = "smtiles";
    protected static final String METADATA_TABLE_NAME = "metadatas";
    private static final String THREEDTILES_METADATAS_COLLECTION = "metadatas_3dTiles";
    public static final String TILERULEVERSION = "1.0";
    private ShivaClient shivaClient;
    private ShivaTableMeta metadataTable;
    private Map<String, ScopeTileset> tilesets = new ConcurrentHashMap<>();
    private ScopeHelper scopeHelper;

    protected void checkConnected() {
        if (!connected.get()) {
            throw new IllegalStateException("切片源没有初始化");
        }
    }
    @Override
    public Tileset<?, ?>[] getTilesets() {
        checkConnected();
        Tileset<?, ?>[] tilesetArray = new Tileset[tilesets.size()];
        return tilesets.values().toArray(tilesetArray);
    }


    @Override
    public void refresh() {
        checkConnected();
        // TODO 加锁 。
        Map<String, TileType> namesAndTypes = readTilesetNamesAndTypes(this.metadataTable);
        // TODO 是不是需要把原来的tileset给dispose 掉。
        this.tilesets.clear();
        this.tilesets.putAll(loadTilesets(namesAndTypes, this.metadataTable));
//        this.tilesets.putAll(realspaceTilesetReader.getTilesets());
    }

    @Override
    public Tileset<?, ?> getTileset(String name) {
        checkConnected();
        if (name == null || name.length() <= 0) {
            return null;
        }
        // String key = name.replace(MongoDBTileset.MONGODBTILESET_NAME_PRIFIX, "");
        return tilesets.get(name);
    }
    
    protected void setMetadataTable(ShivaTableMeta metadataTable) {
        this.metadataTable = metadataTable;
    }

    protected ShivaTableMeta getMetadataTable() {
        return metadataTable;
    }

    protected ShivaClient getShivaClient() {
        return shivaClient;
    }

    protected void setShivaClient(ShivaClient  shivaClient) {
        this.shivaClient = shivaClient;
    }


    @Override
    protected boolean doConnect(ScopeTileSourceInfo tilesourceInfo) {
        if (StringUtils.isBlank(tilesourceInfo.serverAddresses)) {
            throw new IllegalArgumentException("serverAddresses null");
        }
        if (this.scopeHelper == null) {
            this.scopeHelper = new ScopeHelper();
        }
        ShivaClient client = scopeHelper.initShivaClient(tilesourceInfo);
        if (client == null) {
            return false;
        }
        this.shivaClient = client;

        this.metadataTable = scopeHelper.getMetaTable(METADATA_TABLE_NAME);
        if (metadataTable == null) {
            this.metadataTable = scopeHelper.createMetaTable(METADATA_TABLE_NAME);
        }
        Map<String, TileType> namesAndTypes = readTilesetNamesAndTypes(this.metadataTable);
        this.tilesets.putAll(loadTilesets(namesAndTypes, this.metadataTable));
        return true;
    }

    protected Map<String, ScopeTileset> loadTilesets(Map<String, TileType> tilesetNamesAndTypes, ShivaTableMeta metadataTable) {
        return this.scopeHelper.loadTilesets(tilesetNamesAndTypes, metadataTable);
    }

    @Override
    protected Tileset<?, ?> doCreateTileset(MetaData metaData) {
        if (!(metaData instanceof ImageMetaData) && !(metaData instanceof TerrainMetaData) && !(metaData instanceof RealspaceImageMetaData)) {
            throw new UnsupportedOperationException("Mongodb is only support to store image tile");
        }
        String tilesetName = metaData.getTilesetId();
        try {
            // 根据不同的metaData来加载不同的Tileset
            // MongoDBTileset tileset = new MongoDBTileset(tilesetName, this.metadataTable);
            ScopeTileset tileset = this.scopeHelper.newInstanceAndInit(metaData, this.scopeHelper, this.metadataTable);

            this.tilesets.put(tilesetName, tileset);
            return tileset;
        } catch (IllegalArgumentException e) {
            // TODO 回撤上两个操作。
            return null;
        }
    }

//    protected ShivaTableMeta createMetaDataCollection(DB smtilesDB) {
//        // capped 参数表示集合的大小是否有上限 。
//        DBObject parameter = new BasicDBObject().append("capped", false);
//        // 此处，如果parameter为null ,那么真正创建集合的操作将推迟到第一次向该集合中写数据时进行。
//        // TODO 创建索引
//        return smtilesDB.createCollection(METADATA_TABLE_NAME, parameter);
//    }

    @Override
    protected boolean doDisConnect() {
        if (shivaClient != null) {
            shivaClient.close();
        }
        return true;
    }

    /*
     * 从元信息集合中取到所有的切片集名称
     */
    Map<String, TileType> readTilesetNamesAndTypes(ShivaTableMeta metadataTable) {
        if (metadataTable == null) {
            return new HashMap<>();
        }
        Map<String, TileType> tilesetNamesAndTypes = new HashMap<>();
        ShivaScanToken.ShivaScanTokenBuilder tokenBuilder = this.scopeHelper.newScanTokenBuilder(metadataTable);
        List<String> colNameList = new ArrayList<>();
        // 设定需要返回哪些列的数据
        colNameList.add(ScopeTileset.METADATAS_TILESETNAME_FIELDNAME);
        colNameList.add(ScopeTileset.METADATAS_TILESETTYPE_FIELDNAME);
        tokenBuilder.setProjectedColumnNames(colNameList);
        List<ShivaScanToken> tokens = tokenBuilder.build();
        for (ShivaScanToken token : tokens) {
            ShivaScanner shivaScanner = this.scopeHelper.queryByToken(token);
            try {
                while (shivaScanner.hasMoreRows()) {
                    RowResultIterator it = shivaScanner.nextRows();
                    int batchNum = it.getNumRows();
                    for (RowResult result : it) {
                        String tilesetName = result.getString(ScopeTileset.TILESET_NAME_FIELD_NAME);
                        String tilesetType = result.getString(ScopeTileset.METADATAS_TILESETTYPE_FIELDNAME);
                        if (StringUtils.isBlank(tilesetName) || StringUtils.isBlank(tilesetType)) {
                            continue;
                        }
                        tilesetNamesAndTypes.put(tilesetName, TileType.valueOf(tilesetType));
                    }
                }
            } catch (ShivaException e) {
                e.printStackTrace();
            } finally {
                if (shivaScanner != null) {
                    try {
                        shivaScanner.close();
                    } catch (ShivaException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return tilesetNamesAndTypes;
    }

}
