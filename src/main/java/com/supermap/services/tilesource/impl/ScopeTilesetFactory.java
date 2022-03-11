package com.supermap.services.tilesource.impl;

import com.supermap.services.components.commontypes.TileType;
import com.supermap.services.tilesource.ImageMetaData;
import com.supermap.services.tilesource.MVTTileMetaData;
import com.supermap.services.tilesource.MetaData;
import io.transwarp.shiva2.client.RowResult;
import io.transwarp.shiva2.client.RowResultIterator;
import io.transwarp.shiva2.client.ShivaClient;
import io.transwarp.shiva2.client.ShivaScanToken;
import io.transwarp.shiva2.client.ShivaScanner;
import io.transwarp.shiva2.exception.ShivaException;
import io.transwarp.shiva2.metadata.ShivaTableMeta;
import io.transwarp.shiva2.predicate.ShivaColumnPredicate;
import io.transwarp.shiva2.schema.ShivaColumnSchema;
import io.transwarp.shiva2.schema.ShivaSchema;
import shiva2.com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Scope瓦片集工厂类
 * </p>
 *
 * @author ${Author}
 * @version ${Version}
 */
public class ScopeTilesetFactory {
    private static MetaDataFactory metaDataFactory = new MetaDataFactory() {

        @Override
        public ScopeTileset newScopeTileset(String tilesetId, ShivaTableMeta metadataTable) {
            return new ScopeTileset(tilesetId, metadataTable);
        }

        @Override
        public ScopeMVTTileset newScopeMVTTileset(String tilesetId, ShivaTableMeta metadataTable) {
            return new ScopeMVTTileset(tilesetId, metadataTable);
        }
    };


    /**
     * <p>
     * 创建Scope瓦片集实例
     * </p>
     *
     *
     * @param scopeHelper
     * @param tilesetName
     * @param metadataTable
     * @return
     */
    public static ScopeTileset newInstance(ShivaClient client, ScopeHelper scopeHelper, String tilesetName, ShivaTableMeta metadataTable) throws ShivaException {

        ShivaScanToken.ShivaScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(metadataTable);
        ShivaSchema schema = metadataTable.getSchema();
        List<String> colNameList = new ArrayList<>();
        // 设定需要返回哪些列的数据
        colNameList.add(ScopeTileset.METADATAS_TILESETTYPE_FIELDNAME);
        tokenBuilder.setProjectedColumnNames(colNameList);
        // 获取查询条件的列
        ShivaColumnSchema expectedTilesetName = Preconditions.checkNotNull(schema.getColumnIfExist(ScopeTileset.TILESET_NAME_FIELD_NAME));
        // 设定查询条件，这里时uid=xx and rid == xx
        tokenBuilder.addPredicate(ShivaColumnPredicate.newEqualPredicate(expectedTilesetName, tilesetName));
        List<ShivaScanToken> tokens = tokenBuilder.build();
        ShivaScanner shivaScanner = tokens.get(0).intoScanner(client);
        TileType tileType = null;
        while (shivaScanner.hasMoreRows()) {
            RowResultIterator it = shivaScanner.nextRows();
            int batchNum = it.getNumRows();
            for (RowResult result : it) {
                tileType = TileType.valueOf(result.getString(ScopeTileset.METADATAS_TILESETTYPE_FIELDNAME));
            }
        }
        shivaScanner.close();
        if (tileType == null) {
            throw new IllegalArgumentException("Init failed!");
        }
        ScopeTileset tileset = null;
        switch (tileType) {
            case Vector:
                tileset = new ScopeMVTTileset(tilesetName, scopeHelper, metadataTable);
                break;
            case Image:
            default:
                tileset = new ScopeTileset(tilesetName, scopeHelper, metadataTable);
        }
        return tileset;
    }

    /**
     * <p>
     * 创建及初始化Scope瓦片集实例
     * </p>
     *
     * @param metaData
     * @param scopeHelper
     * @return
     */
    public static ScopeTileset newInstanceAndInit(MetaData metaData, ScopeHelper scopeHelper, ShivaTableMeta metadataTable) {
        boolean initSuccess = false;
        ScopeTileset tileset = null;
        // MVTTileMetaData是ImageMetaData的子类
        if (metaData instanceof MVTTileMetaData) {
            ScopeMVTTileset ScopeMVTTileset = metaDataFactory.newScopeMVTTileset(metaData.getTilesetId(), metadataTable);
            initSuccess = ScopeMVTTileset.init(metaData, scopeHelper, metadataTable);
            tileset = ScopeMVTTileset;
        } else if (metaData instanceof ImageMetaData) {
            ScopeTileset ScopeTileset = metaDataFactory.newScopeTileset(metaData.getTilesetId(), metadataTable);
            initSuccess = ScopeTileset.init(metaData, scopeHelper, metadataTable);
            tileset = ScopeTileset;
        }
        if (!initSuccess) {
            throw new IllegalArgumentException("Init failed!");
        }
        return tileset;
    }

    public static ScopeTileset newInstance(String tilesetName, TileType tileType, ShivaTableMeta metadataTable) {
        ScopeTileset tileset = null;
        switch (tileType) {
            case Vector:
                tileset = new ScopeMVTTileset(tilesetName, metadataTable);
                break;
            case Image:
            default:
                tileset = new ScopeTileset(tilesetName, metadataTable);
        }
        return tileset;
    }

    interface MetaDataFactory {
        ScopeTileset newScopeTileset(String tilesetId, ShivaTableMeta metadataTable);

        ScopeMVTTileset newScopeMVTTileset(String tilesetId, ShivaTableMeta metadataTable);
    }

    public static void setMetaDataFactory(MetaDataFactory metaDataFactory) {
        ScopeTilesetFactory.metaDataFactory = metaDataFactory;
    }

}