package com.supermap.services.tilesource.impl;

import com.supermap.services.components.commontypes.TileType;
import com.supermap.services.tilesource.MetaData;
import com.supermap.services.tilesource.ScopeTileSourceInfo;
import io.transwarp.shiva2.client.ShivaClient;
import io.transwarp.shiva2.client.ShivaScanToken;
import io.transwarp.shiva2.client.ShivaScanner;
import io.transwarp.shiva2.client.ShivaSession;
import io.transwarp.shiva2.client.ShivaTableCreator;
import io.transwarp.shiva2.common.Options;
import io.transwarp.shiva2.common.ShivaContext;
import io.transwarp.shiva2.common.ShivaCustomProperty;
import io.transwarp.shiva2.common.ShivaDatabase;
import io.transwarp.shiva2.common.ShivaEngineType;
import io.transwarp.shiva2.common.ShivaTableId;
import io.transwarp.shiva2.common.ShivaTableName;
import io.transwarp.shiva2.common.Status;
import io.transwarp.shiva2.exception.ShivaException;
import io.transwarp.shiva2.metadata.ShivaTableMeta;
import io.transwarp.shiva2.schema.ShivaSchema;
import io.transwarp.shiva2.schema.ShivaSchemaBuilder;
import io.transwarp.shiva2.types.ShivaDataType;
import shiva2.com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ScopeHelper {

    private ShivaClient client;

    private static final String TILE_DATABASE_NAME = "tilesets";

    public static final String TILESET_VERSION_NAME = "name";
    // tiles表名
    protected static final String TILES_TABLE_PRIFIX = "tiles";
    // images 表名
    protected static final String IMAGES_TABLE_PRIFIX = "images";
    protected static final String TILE_LOCATION_FIELDNAME = "tile_location";
    protected static final String TILE_COLUMN_FIELDNAME = "tile_column";
    protected static final String TILE_ROW_FIELDNAME = "tile_row";
    protected static final String TILE_RESOLUTION_FIELDNAME = "resolution";
    protected static final String CREATE_TIME_FIELDNAME = "create_time";
    protected static final String TILE_ID_FIELDNAME = "tile_id";
    protected static final String TILE_DATE_FIELDNAME = "tile_data";
    protected static final String TILE_FORMATNAME = "tile_formatName";
    // meta 表名
    protected static final String METADATAS_TABLE_NAME = "metadatas";
    protected static final String METADATAS_TILESETNAME_FIELDNAME = "tilesetName";
    protected static final String METADATAS_TILESETTYPE_FIELDNAME = "tilesetType";
    protected static final String METADATAS_METAINFO_FIELDNAME = "metaInfo";
    protected static final String METADATAS_VERSIONS_FIELDNAME = "tileversions";
    // gridfs 表名
    // protected static final String FS_FILES = "fs.files";

    ShivaClient initShivaClient(ScopeTileSourceInfo scopeTileSourceInfo) {
        String serverAddresses = scopeTileSourceInfo.serverAddresses;
        Options options = new Options();
        options.masterGroup = serverAddresses;
        options.maxErrorRetry = 5;
        ShivaClient client = ShivaClient.getInstance();
        try {
            client.start(options);
            this.client = client;
            ShivaContext<Map<String, ShivaDatabase>> mapShivaContext = client.listDatabases();
            if (!mapShivaContext.getStatus().ok()) {
                client.close();
                return null;
            }
            Map<String, ShivaDatabase> resource = mapShivaContext.getResource();
            Set<String> databaseNames = resource.keySet();
            if (!databaseNames.contains(TILE_DATABASE_NAME)) {
                Map<String, ShivaCustomProperty> customProperties = new HashMap<>();
                Status status = client.createDatabase(TILE_DATABASE_NAME, customProperties);
                if (!status.ok()) {
                    client.close();
                    return null;
                }
            }
            return client;
        } catch (ShivaException e) {
            e.printStackTrace();
        }
        client.close();
        return null;
    }

    /**
     * <p>
     * 创建瓦片集表
     * </p>
     *
     * @param name
     * @return
     */
    public ShivaTableMeta createTilesTable(String name) {
        String tableName = TILES_TABLE_PRIFIX + "_" + name;
        ShivaSchemaBuilder schemaBuilder = new ShivaSchemaBuilder();

        schemaBuilder.addColumn(TILE_LOCATION_FIELDNAME, ShivaDataType.STRING).notNull();
        schemaBuilder.addColumn(TILE_ID_FIELDNAME, ShivaDataType.STRING).notNull();
        schemaBuilder.addColumn(TILE_COLUMN_FIELDNAME, ShivaDataType.INT).notNull();
        schemaBuilder.addColumn(TILE_ROW_FIELDNAME, ShivaDataType.INT).notNull();
        schemaBuilder.addColumn(TILE_RESOLUTION_FIELDNAME, ShivaDataType.DOUBLE).notNull();
        schemaBuilder.addColumn(TILESET_VERSION_NAME, ShivaDataType.STRING).notNull();
        schemaBuilder.addColumn(TILE_FORMATNAME, ShivaDataType.STRING).notNull();
        schemaBuilder.addColumn(CREATE_TIME_FIELDNAME, ShivaDataType.BIGINT).notNull();
        // 设定该表主键
        schemaBuilder.setPrimaryKeys(Lists.newArrayList(TILE_LOCATION_FIELDNAME));
        ShivaSchema shivaSchema = schemaBuilder.buildOrDead();
        return this.createTable(tableName, shivaSchema, TILE_LOCATION_FIELDNAME);
    }

    /**
     * <p>
     * 创建图片集表
     * </p>
     *
     * @param name
     * @return
     */
    public ShivaTableMeta createImagesTable(String name) {
        String tableName = IMAGES_TABLE_PRIFIX + "_" + name;
        ShivaSchemaBuilder schemaBuilder = new ShivaSchemaBuilder();
        schemaBuilder.addColumn(TILE_ID_FIELDNAME, ShivaDataType.STRING).notNull();
        schemaBuilder.addColumn(TILE_DATE_FIELDNAME, ShivaDataType.BINARY).notNull();
        // 设定该表主键
        schemaBuilder.setPrimaryKeys(Lists.newArrayList(TILE_ID_FIELDNAME));
        ShivaSchema shivaSchema = schemaBuilder.buildOrDead();
        return this.createTable(tableName, shivaSchema, TILE_ID_FIELDNAME);
    }
    /**
     * <p>
     * 创建元信息表
     * </p>
     *
     * @param name
     * @return
     */
    public ShivaTableMeta createMetaTable(String name) {
        ShivaSchemaBuilder schemaBuilder = new ShivaSchemaBuilder();
        schemaBuilder.addColumn(METADATAS_TILESETNAME_FIELDNAME, ShivaDataType.STRING).notNull();
        schemaBuilder.addColumn(METADATAS_METAINFO_FIELDNAME, ShivaDataType.STRING).notNull();
        schemaBuilder.addColumn(METADATAS_TILESETTYPE_FIELDNAME, ShivaDataType.STRING).notNull();
        schemaBuilder.addColumn(METADATAS_VERSIONS_FIELDNAME, ShivaDataType.STRING).notNull();
        // 设定该表主键
        schemaBuilder.setPrimaryKeys(Lists.newArrayList(METADATAS_TILESETNAME_FIELDNAME));
        ShivaSchema shivaSchema = schemaBuilder.buildOrDead();
        return this.createTable(name, shivaSchema, METADATAS_TILESETNAME_FIELDNAME);
    }

    public ShivaTableMeta createTable(String tableName, ShivaSchema shivaSchema, String... primaryKey) {
        ShivaTableCreator creator = client.newTableCreator();
        Status status =
                creator.addPrimaryHashPartition(Lists.newArrayList(primaryKey), 12)
                        .setTableName(tableName)
                        .setDatabaseName(TILE_DATABASE_NAME)
                        .setSchema(shivaSchema)
                        .setEngineType(ShivaEngineType.TAB)
                        .setNumReplicas(3)
                        .setTimeout(30, TimeUnit.SECONDS)
                        .create();
        ShivaTableId createdTableId = creator.getCreatedTableId();
        return client.openTableOrDead(createdTableId);
    }


    public ShivaTableMeta getTilesTable(String tilesetName) {
        String tableName = TILES_TABLE_PRIFIX + "_" + tilesetName;
        ShivaTableName shivaTableName = ShivaTableName.newName(TILE_DATABASE_NAME, tableName);
        ShivaContext<ShivaTableMeta> shivaTableMetaShivaContext = client.openTable(shivaTableName);
        if (shivaTableMetaShivaContext == null || shivaTableMetaShivaContext.getResource() == null) {
            return null;
        }
        return shivaTableMetaShivaContext.getResource();
    }

    public ShivaTableMeta getImagesTable(String tilesetName) {
        String tableName = IMAGES_TABLE_PRIFIX + "_" + tilesetName;
        ShivaTableName shivaTableName = ShivaTableName.newName(TILE_DATABASE_NAME, tableName);
        ShivaContext<ShivaTableMeta> shivaTableMetaShivaContext = client.openTable(shivaTableName);
        if (shivaTableMetaShivaContext == null || shivaTableMetaShivaContext.getResource() == null) {
            return null;
        }
        return shivaTableMetaShivaContext.getResource();
    }

    public ShivaTableMeta getMetaTable(String metaTableName) {
        return getTable(metaTableName);
    }

    public ShivaTableMeta getTable(String tableName) {
        ShivaTableName shivaTableName = ShivaTableName.newName(TILE_DATABASE_NAME, tableName);
        ShivaContext<ShivaTableMeta> shivaTableMetaShivaContext = client.openTable(shivaTableName);
        if (shivaTableMetaShivaContext == null || shivaTableMetaShivaContext.getResource() == null) {
            return null;
        }
        return shivaTableMetaShivaContext.getResource();
    }
    public ShivaSession newSession() {
        return client.newSession();
    }

    public ScopeTileset newInstanceAndInit(MetaData metaData, ScopeHelper scopeHelper, ShivaTableMeta metadataTable) {
        return ScopeTilesetFactory.newInstanceAndInit(metaData, scopeHelper, metadataTable);
    }

    public ScopeTileset newInstanceTileset(String tilesetName, TileType value, ScopeHelper scopeHelper, ShivaTableMeta metaDataTable) {
        try {
            return ScopeTilesetFactory.newInstance(client, scopeHelper, tilesetName, metaDataTable);
        } catch (ShivaException e) {
            e.printStackTrace();
        }
        return null;
    }
    public ScopeTileset newInstanceTileset(String tilesetName, TileType tileType, ShivaTableMeta metadataTable) {
        return ScopeTilesetFactory.newInstance(tilesetName, tileType, metadataTable);
    }


    public ShivaScanToken.ShivaScanTokenBuilder newScanTokenBuilder(ShivaTableMeta shivaTableMeta) {
        return client.newScanTokenBuilder(shivaTableMeta);
    }

    public ShivaScanner queryByToken(ShivaScanToken shivaScanToken) {
        return shivaScanToken.intoScanner(client);
    }

    public Map<String, ScopeTileset> loadTilesets(Map<String, TileType> tilesetNamesAndTypes, ShivaTableMeta metadataTable) {
        Map<String, ScopeTileset> tilesets = new HashMap<>();
        tilesetNamesAndTypes.forEach((key, value) -> {
            ScopeTileset tileset = newInstanceTileset(key, value, this, metadataTable);
            tileset.init(metadataTable);
            tilesets.put(key, tileset);
        });
        return tilesets;
    }

}
