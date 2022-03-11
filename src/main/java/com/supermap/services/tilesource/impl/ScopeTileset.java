package com.supermap.services.tilesource.impl;

import com.supermap.services.components.commontypes.PureColorInfo;
import com.supermap.services.components.commontypes.Rectangle2D;
import com.supermap.services.rest.util.JsonConverter;
import com.supermap.services.tilesource.ImageMetaData;
import com.supermap.services.tilesource.ImageTileInfo;
import com.supermap.services.tilesource.MBTilesUtil;
import com.supermap.services.tilesource.MVTTileMetaData;
import com.supermap.services.tilesource.MathUtil;
import com.supermap.services.tilesource.MetaData;
import com.supermap.services.tilesource.PutTileFailedException;
import com.supermap.services.tilesource.Tile;
import com.supermap.services.tilesource.TileVersion;
import com.supermap.services.tilesource.TileVersionList;
import com.supermap.services.tilesource.VectorMetaData;
import com.supermap.services.tilesource.VersionUpdate;
import io.transwarp.shiva2.client.Operation;
import io.transwarp.shiva2.client.RowResult;
import io.transwarp.shiva2.client.RowResultIterator;
import io.transwarp.shiva2.client.SessionConfiguration;
import io.transwarp.shiva2.client.ShivaScanToken;
import io.transwarp.shiva2.client.ShivaScanner;
import io.transwarp.shiva2.client.ShivaSession;
import io.transwarp.shiva2.client.ShivaUpsert;
import io.transwarp.shiva2.exception.ShivaException;
import io.transwarp.shiva2.metadata.ShivaTableMeta;
import io.transwarp.shiva2.predicate.ShivaColumnPredicate;
import io.transwarp.shiva2.schema.PartialRow;
import io.transwarp.shiva2.schema.ShivaColumnSchema;
import io.transwarp.shiva2.schema.ShivaSchema;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import shiva2.com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

//定义了该规范对应的切片集。该类根据标准中定义的格式，实现了瓦片的存储与管理。 
public class ScopeTileset extends AbstractImageTileset {

    private volatile ArrayBlockingQueue<String> existPureColorIds = new ArrayBlockingQueue<>(2000);
    protected String name;

    public static final String TILESET_NAME_FIELD_NAME = "tilesetName";
    public static final String TILESET_VERSION_NAME = "name";
    // tiles表名
    public static final String TILES_COLLECTION_PRIFIX = "tiles";
    // images 表名
    public static final String IMAGES_COLLECTION_PRIFIX = "images";
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
    public static final String METADATAS_TILESETNAME_FIELDNAME = "tilesetName";
    public static final String METADATAS_TILESETTYPE_FIELDNAME = "tilesetType";
    protected static final String METADATAS_METAINFO_FIELDNAME = "metaInfo";
    public static final String METADATAS_VERSIONS_FIELDNAME = "tileversions";

    protected ShivaTableMeta metadataTable;
    protected ShivaTableMeta tilesTable;
    protected ShivaTableMeta imagesTable;
    protected ScopeHelper scopeHelper;

    private ReentrantLock metaDataLock = new ReentrantLock();

    private TileVersionList tileVersions;

    protected ImageMetaData metaData;

    @Override
    public void setTileVersions(TileVersionList tileVersions) {
        this.tileVersions = tileVersions;
    }
    /**
     * <p>
     * 获取切片版本
     * </p>
     * @return
     * @since 8.1.1
     */
    public TileVersionList getTileVersions() {
        return tileVersions;
    }

    protected boolean tileVersionSupported() {
        return this.tileVersions != null;
    }
    /**
     * <p>
     * 获取切片版本
     * </p>
     * @return
     */
    public List<TileVersion> getVersions() {
        if (!this.tileVersionSupported()) {
            return null;
        }
        List<TileVersion> versions = new ArrayList<TileVersion>();
        versions.addAll(Arrays.asList(this.tileVersions.toArray()));
        return versions;
    }

    @Override
    public ImageMetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(ImageMetaData metaData) {
        this.metaData = metaData;
    }

    public ScopeTileset(String tilesetName, ScopeHelper scopeHelper, ShivaTableMeta metadataTable) {
        this.name = tilesetName;
        this.scopeHelper = scopeHelper;
        this.metadataTable = metadataTable;
    }

    /**
     * <p>
     * 构造函数。
     * </p>
     *
     * @param name          tileset名称
     * @param metadataTable 元信息表
     */
    public ScopeTileset(String name, ShivaTableMeta metadataTable) {
        this.name = name;
        this.metadataTable = metadataTable;
    }

    /**
     * <p>
     * 切片集信息存在于MongoDB时的初始化调用。
     * </p>
     *
     * @return
     * @since 7.0.1
     */
    public boolean init(ShivaTableMeta metadataTable) {
        setMetaData(readMetaData(this.name, metadataTable));
        if (this.metaData instanceof ImageMetaData) {
            TileVersion[] tileversions = readVersion(this.name, this.metadataTable);
            if (tileversions != null) {
                setTileVersions(new TileVersionList(tileversions));
            }
        }
        this.imagesTable = scopeHelper.getImagesTable(this.name);
        this.tilesTable = scopeHelper.getTilesTable(this.name);
        return true;
    }

    /**
     * <p>
     * 切片集信息还不存在于MongoDB时的初始化调用。
     * </p>
     *
     * @param metaData
     * @param scopeHelper
     * @param metadataTable
     * @return
     * @since 7.0.1
     */
    public boolean init(MetaData metaData, ScopeHelper scopeHelper, ShivaTableMeta metadataTable) {
        setMetaData((ImageMetaData) metaData);
        if (this.scopeHelper == null) {
            this.scopeHelper = scopeHelper;
            this.metadataTable = metadataTable;
        }
        if (metaData instanceof ImageMetaData) {
            this.setTileVersions(new TileVersionList());
            String versionName = "V1";
            TileVersion originalVersion = this.createTileVersion(versionName, null,
                    new VersionUpdate(metaData.bounds, metaData.scaleDenominators, metaData.resolutions));
        }
        upsetMetaDataToTable(metaData, this.tileVersions);
        ShivaTableMeta tilesTable = this.scopeHelper.getTilesTable(this.name);
        if (tilesTable == null) {
            this.tilesTable = this.scopeHelper.createTilesTable(this.name);
        } else {
            this.tilesTable = tilesTable;
        }

        ShivaTableMeta imagesTable = this.scopeHelper.getImagesTable(this.name);
        if (imagesTable == null) {
            this.imagesTable = this.scopeHelper.createImagesTable(this.name);
        } else {
            this.imagesTable = imagesTable;
        }
        return true;
    }

    /**
     * <p>
     * 创建一个新的版本号
     * </p>
     * @param desc
     * @param parent
     * @param update
     * @return
     */
    public TileVersion createTileVersion(String desc, String parent, VersionUpdate update) {
        boolean versionSupported = this.tileVersionSupported();
        if (!versionSupported) {
            return null;
        }
        TileVersion tileVersion = new TileVersion(generateVersionName(), desc, parent, update, System.currentTimeMillis());
        this.tileVersions.add(tileVersion);
        if (this.tileVersions.size() > 1) {
            upsetMetaDataToTable(this.metaData, this.tileVersions);
        }
        return tileVersion;
    }

    TileVersion[] readVersion(String name, ShivaTableMeta metadataTable) {

        ShivaScanToken.ShivaScanTokenBuilder tokenBuilder = this.scopeHelper.newScanTokenBuilder(metadataTable);
        List<String> colNameList = new ArrayList<>();
        // 设定需要返回哪些列的数据
        colNameList.add(ScopeTileset.METADATAS_VERSIONS_FIELDNAME);
        tokenBuilder.setProjectedColumnNames(colNameList);
        // 获取查询条件的列
        ShivaSchema schema = metadataTable.getSchema();
        ShivaColumnSchema tilesetName = Preconditions.checkNotNull(schema.getColumnIfExist(TILESET_NAME_FIELD_NAME));
        // 设定查询条件，这里时uid=xx and rid == xx
        tokenBuilder.addPredicate(ShivaColumnPredicate.newEqualPredicate(tilesetName, name));
        List<ShivaScanToken> tokens = tokenBuilder.build();
        for (ShivaScanToken token : tokens) {
            ShivaScanner shivaScanner = this.scopeHelper.queryByToken(token);
            try {
                while (shivaScanner.hasMoreRows()) {
                    RowResultIterator it = shivaScanner.nextRows();
                    int batchNum = it.getNumRows();
                    for (RowResult result : it) {
                        String tileversions = result.getString(ScopeTileset.METADATAS_VERSIONS_FIELDNAME);
                        if (StringUtils.isBlank(tileversions)) {
                            continue;
                        }
                        return prase(tileversions, TileVersion[].class);                    }
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
        return null;
    }

    protected String generateVersionName() {
        return UUID.randomUUID().toString();
    }

    void upsetMetaDataToTable(MetaData metaData, TileVersionList tileVersionList) {
        ShivaTableMeta metaTable = scopeHelper.getMetaTable(METADATAS_TABLE_NAME);
        if (metaTable == null) {
            ShivaTableMeta newMetaTable = scopeHelper.createMetaTable(METADATAS_TABLE_NAME);
            if (newMetaTable == null) {
                throw new IllegalStateException("创建metaTable失败");
            }
            this.metadataTable = newMetaTable;
        } else {
            this.metadataTable = metaTable;
        }
        ShivaSession session = this.scopeHelper.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        session.setMaxMutationBufferNum(2);
        session.setMutationBufferSpace(7 * 1024 * 1024);
        String versionsText = JsonConverter.toJson(tileVersionList.toArray());
        String metaText = JsonConverter.toJson(metaData);
        Operation op = metadataTable.newUpsert(ShivaUpsert.UpsertType.UPSERT_WITH_Merge);
        PartialRow row = op.getRow();
        row.addString(TILESET_NAME_FIELD_NAME, name);
        row.addString(METADATAS_METAINFO_FIELDNAME, metaText);
        if (metaData instanceof ImageMetaData) {
            row.addString(METADATAS_TILESETTYPE_FIELDNAME, "Image");
        } else if (metaData instanceof VectorMetaData) {
            row.addString(METADATAS_TILESETTYPE_FIELDNAME, "Vector");
        }
        if (metaData instanceof ImageMetaData && tileVersionList != null) {
            // mvt元数据需要保留版本
            if (metaData instanceof MVTTileMetaData) {
                ((MVTTileMetaData) metaData).tileversions = tileVersionList.toArray();
            }
            row.addString(METADATAS_VERSIONS_FIELDNAME, versionsText);
        }
        try {
            session.apply(op);
        } catch (ShivaException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
            } catch (ShivaException e) {
                e.printStackTrace();
            }
        }
    }

    private ImageMetaData readMetaData(String name, ShivaTableMeta metadataTable) {
        ShivaScanToken.ShivaScanTokenBuilder tokenBuilder = this.scopeHelper.newScanTokenBuilder(metadataTable);
        List<String> colNameList = new ArrayList<>();
        // 设定需要返回哪些列的数据
        colNameList.add(ScopeTileset.METADATAS_METAINFO_FIELDNAME);
        tokenBuilder.setProjectedColumnNames(colNameList);
        // 获取查询条件的列
        ShivaSchema schema = metadataTable.getSchema();
        ShivaColumnSchema tilesetName = Preconditions.checkNotNull(schema.getColumnIfExist(TILESET_NAME_FIELD_NAME));
        // 设定查询条件，这里时uid=xx and rid == xx
        tokenBuilder.addPredicate(ShivaColumnPredicate.newEqualPredicate(tilesetName, name));
        List<ShivaScanToken> tokens = tokenBuilder.build();
        for (ShivaScanToken token : tokens) {
            ShivaScanner shivaScanner = this.scopeHelper.queryByToken(token);
            try {
                while (shivaScanner.hasMoreRows()) {
                    RowResultIterator it = shivaScanner.nextRows();
                    int batchNum = it.getNumRows();
                    for (RowResult result : it) {
                        String metaInfo = result.getString(ScopeTileset.METADATAS_METAINFO_FIELDNAME);
                        if (StringUtils.isBlank(metaInfo)) {
                            continue;
                        }
                        return prase(metaInfo, ImageMetaData.class);
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
        return null;
    }

    private <I> I prase(String metaInfo, Class<I> targetClass) {
        try {
            return new JsonConverter().to(metaInfo, targetClass);
        } catch (JSONException e) {
            return null;
        }
    }

    //此方法用于更新 Tileset 的元信息，由于该规范中并没有这个概念，所以此方法不用实现。 
    @Override
    protected boolean doUpdateMetaData(ImageMetaData metaData, TileVersionList tileVersions) {
        return false;
    }

    //获取瓦片，根据规范命名。 
    @Override
    public ImageTileInfo get(Tile tile) {
        checkTile(tile);
        TileVersionList tileVersionsNow = this.getTileVersions();
        boolean isTileVersionSupported = tileVersionsNow != null;
        if (isTileVersionSupported) {
            tile.version = tileVersionsNow.getActualVersion(metaData.originalPoint, metaData.tileWidth, tile);
        }
        rectifyTileResolution(tile);
        // 优先根据tile信息拼接出tile_id然后从images中获取iamgeData信息，如果imageTileInfo为空（有可能是白图）尝试根据从tiles表和images表中取出图片信息。
        String tile_id = this.getTileID(tile);
        ImageTileInfo imageTileInfo = this.getImageInfoUseTileID(tile, tile_id, System.currentTimeMillis(), this.metaData.tileFormat.toString());
        if (imageTileInfo != null) {
            return imageTileInfo;
        }
        return getImageInfoUseXYZ(tile);
    }

    private void rectifyTileResolution(Tile tile) {
        tile.resolution = this.getActualResolution(tile.resolution);
    }

    protected double getActualResolution(double resolution) {
        MetaData metaData = this.getMetaData();
        if (metaData == null || ArrayUtils.isEmpty(metaData.resolutions)) {
            return resolution;
        }
        for (double resInMetaData : metaData.resolutions) {
            if (Math.abs(resInMetaData - resolution) <= 1.0E-8) {
                return resInMetaData;
            }
        }
        return resolution;
    }

    private ImageTileInfo getImageInfoUseTileID(Tile tile, String tile_id, long create_time, String formatName) {
        if (StringUtils.isEmpty(tile_id)) {
            return null;
        }
        if (StringUtils.isNotEmpty(formatName)) {
            tile.formatName = formatName;
        }
        ByteBuffer imagesByte = getTileDataFromImagesTable(tile_id);
        if (imagesByte == null) {
            return null;
        }
        byte[] imageData = null;
        try {
            imageData = imagesByte.array();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ImageTileInfo(tile, imageData, create_time);
    }

    /**
     * 取切片时从image表中获取切片数据
     */
    protected ByteBuffer getTileDataFromImagesTable(String tile_id) {
        ShivaScanToken.ShivaScanTokenBuilder tokenBuilder = this.scopeHelper.newScanTokenBuilder(this.imagesTable);
        List<String> colNameList = new ArrayList<>();
        // 设定需要返回哪些列的数据
        colNameList.add(ScopeTileset.TILE_DATE_FIELDNAME);
        tokenBuilder.setProjectedColumnNames(colNameList);
        ShivaSchema schema = imagesTable.getSchema();
        // 获取查询条件的列
        ShivaColumnSchema expectedTileId = Preconditions.checkNotNull(schema.getColumnIfExist(ScopeTileset.TILE_ID_FIELDNAME));
        // 设定查询条件
        tokenBuilder.addPredicate(ShivaColumnPredicate.newEqualPredicate(expectedTileId, tile_id));
        List<ShivaScanToken> tokens = tokenBuilder.build();
        for (ShivaScanToken token : tokens) {
            ByteBuffer binary = null;
            ShivaScanner shivaScanner = this.scopeHelper.queryByToken(token);
            try {
                while (shivaScanner.hasMoreRows()) {
                    RowResultIterator it = shivaScanner.nextRows();
                    int batchNum = it.getNumRows();
                    for (RowResult result : it) {
                        binary = result.getBinary(ScopeTileset.TILE_DATE_FIELDNAME);
                        if (binary == null) {
                            continue;
                        }
                        return binary;
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
        return null;
    }

    /**
     * <p>
     * 根据tile信息，从tiles表和images表中取出图片信息。
     * 实现方式是：从tiles表中获取tile_id和create_time，formatName
     * 根据拿到的tile_id从images表中获取iamgedata
     * </p>
     *
     * @param tile
     * @return
     * @since 8.1.1
     */
    private ImageTileInfo getImageInfoUseXYZ(Tile tile) {
        RowResult tileInfo = getTileInfoFromTilesTable(tile);
        if (tileInfo == null) {
            return null;
        }
        String tile_id = tileInfo.getString(TILE_ID_FIELDNAME);
        if (StringUtils.isEmpty(tile_id)) {
            return null;
        }
        long create_time = tileInfo.getLong(CREATE_TIME_FIELDNAME);
        String formatName = tileInfo.getString(TILE_FORMATNAME);
        return this.getImageInfoUseTileID(tile, tile_id, create_time, formatName);
    }

    /**
     * 取切片时从Tile表中获取切片信息0
     */
    protected RowResult getTileInfoFromTilesTable(Tile tile) {
        ShivaScanToken.ShivaScanTokenBuilder tokenBuilder = this.scopeHelper.newScanTokenBuilder(this.tilesTable);
        double actualRes = getActualResolution(tile.resolution);
        double resolutionRadio = getResolutionRatio(actualRes);
        List<String> colNameList = new ArrayList<>();
        // 设定需要返回哪些列的数据
        colNameList.add(ScopeTileset.TILE_ID_FIELDNAME);
        colNameList.add(ScopeTileset.CREATE_TIME_FIELDNAME);
        colNameList.add(ScopeTileset.TILESET_VERSION_NAME);
        colNameList.add(ScopeTileset.TILE_FORMATNAME);
        tokenBuilder.setProjectedColumnNames(colNameList);
        ShivaSchema schema = tilesTable.getSchema();
        // 获取查询条件的列
        ShivaColumnSchema expectedTileLocation = Preconditions.checkNotNull(schema.getColumnIfExist(ScopeTileset.TILE_LOCATION_FIELDNAME));
        // 设定查询条件
        tokenBuilder.addPredicate(ShivaColumnPredicate.newEqualPredicate(expectedTileLocation, resolutionRadio + "_" + tile.x + "_" + tile.y));
        List<ShivaScanToken> tokens = tokenBuilder.build();
        String tileID = null;
        for (ShivaScanToken token : tokens) {
            ShivaScanner shivaScanner = this.scopeHelper.queryByToken(token);
            try {
                while (shivaScanner.hasMoreRows()) {
                    RowResultIterator it = shivaScanner.nextRows();
                    int batchNum = it.getNumRows();
                    for (RowResult result : it) {
                        tileID = result.getString(ScopeTileset.TILE_ID_FIELDNAME);
                        if (StringUtils.isBlank(tileID)) {
                            continue;
                        }
                        return result;
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
        return null;
    }

    //获取存储的瓦片集的名称。
    @Override
    public String getName() {

        return null;
    }

    //向生成的瓦片集中增加瓦片，即把瓦片存储到指定的位置。 
    @Override
    public void put(ImageTileInfo tileInfo) throws PutTileFailedException {
        checkTile(tileInfo);
        byte[] imagesData = tileInfo.tileData;
        if (imagesData == null) {
            return;
        }
        TileVersionList tileVersions = this.getTileVersions();
        if (tileInfo.version != null) {
            tileInfo.version = tileVersions.getActualVersion(metaData.originalPoint, metaData.tileWidth, tileInfo);
        } else {
            /**
             * 在最开始写入瓦片缓存时，瓦片的版本为空，导致存的时候tile_id如78271.516964_0_0_null；
             * 而在读取瓦片时是根据瓦片的版本来读取的，如78271.516964_0_0_f8a8f3b3-2e3c-4e35-b4d1-00bdf05ead00；
             * 以至于一直读取缓存失败，每次都需要重新出图
             */
            tileInfo.version = tileVersions.getLastVersion().name;
        }
        //分布式切图中生成的纯色图片都是用一段信息来表示的,这里先判断是否是纯色图片,如果是的话就使用工具方法获取这段信息代表的图片。
        boolean distributedPureImage = MBTilesUtil.isDistributedPureImage(imagesData);
        String tileID = this.getTileID(distributedPureImage, imagesData, tileInfo);
        putTileToTileTable(tileID, tileInfo);
        if (distributedPureImage) {
            if (this.existPureColorIds.contains(tileID)) {
                return;
            }
            imagesData = MBTilesUtil.transformPureImageToCommonImageData(imagesData);
        }
        putTileToImageTable(tileID, tileInfo, imagesData);
        if (distributedPureImage) {
            addExistPureColorIds(tileID);
        }
    }

    private void putTileToTileTable(String tileID, ImageTileInfo tile) {
        if (this.tilesTable == null) {
            this.tilesTable = scopeHelper.getTilesTable(this.name);
        }
        Operation op = tilesTable.newUpsert(ShivaUpsert.UpsertType.UPSERT_WITH_Merge);
        ShivaSession session = scopeHelper.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        session.setMaxMutationBufferNum(2);
        session.setMutationBufferSpace(7 * 1024 * 1024);
        PartialRow row = op.getRow();
        double resolutionRatio = getResolutionRatio(tile.resolution);
        row.addString(TILE_LOCATION_FIELDNAME, resolutionRatio + "_" + tile.x + "_" + tile.y);
        row.addString(TILE_ID_FIELDNAME, tileID);
        row.addDouble(TILE_RESOLUTION_FIELDNAME, resolutionRatio);
        row.addInt(TILE_COLUMN_FIELDNAME, (int) tile.x);
        row.addInt(TILE_ROW_FIELDNAME, (int) tile.y);
        row.addLong(CREATE_TIME_FIELDNAME, System.currentTimeMillis());
        row.addString(TILESET_VERSION_NAME, tile.version);
        row.addString(TILE_FORMATNAME, tile.formatName);
        try {
            session.apply(op);
        } catch (ShivaException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
            } catch (ShivaException e) {
                e.printStackTrace();
            }
        }
    }

    private void putTileToImageTable(String tileID, ImageTileInfo tileInfo, byte[] imagesData) {
        if (this.imagesTable == null) {
            this.imagesTable = scopeHelper.getImagesTable(this.name);
        }
        Operation op = imagesTable.newUpsert(ShivaUpsert.UpsertType.UPSERT_WITH_Merge);
        ShivaSession session = scopeHelper.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        session.setMaxMutationBufferNum(2);
        session.setMutationBufferSpace(7 * 1024 * 1024);
        PartialRow row = op.getRow();
        row.addString(TILE_ID_FIELDNAME, tileID);
        row.addBinary(TILE_DATE_FIELDNAME, imagesData);

        try {
            session.apply(op);
        } catch (ShivaException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
            } catch (ShivaException e) {
                e.printStackTrace();
            }
        }
    }

    private String getTileID(boolean distributedPureImage, byte[] data, ImageTileInfo tileInfo) {
        if (distributedPureImage) {
            PureColorInfo pureColorInfo = MBTilesUtil.getPureColorInfo(data);
            return MBTilesUtil.getTileIdByRGB(pureColorInfo.rgb, pureColorInfo.hasAlpha);
        }
        return this.getTileID(tileInfo);
    }

    /**
     * <p>
     * 获取标度为6的分辨率
     * </p>
     *
     * @param resolution
     * @return
     */
    public static double getResolutionRatio(double resolution) {
        final int resolutionPrecision = 6;
        BigDecimal tmpResolution = new BigDecimal(resolution);
        return tmpResolution.setScale(resolutionPrecision, RoundingMode.HALF_UP).doubleValue();
    }

    private void checkTile(Tile tile) {
        if (tile == null) {
            throw new IllegalArgumentException("tile cannot be null!");
        }
    }

    private String getTileID(Tile tile) {
        return String.format("%s_%d_%d_%s", MBTilesUtil.getResolutionString(tile.resolution), tile.x, tile.y, tile.version);
    }

    private void addExistPureColorIds(String tileId) {
        try {
            this.existPureColorIds.add(tileId);
        } catch (IllegalStateException e) {
            synchronized (this) {
                try {
                    this.existPureColorIds.add(tileId);
                } catch (IllegalStateException ex) {
                    ArrayBlockingQueue<String> newExistPureColorIDs = new ArrayBlockingQueue<String>(this.existPureColorIds.size() * 2);
                    newExistPureColorIDs.addAll(this.existPureColorIds);
                    this.existPureColorIds = newExistPureColorIDs;
                    this.existPureColorIds.add(tileId);
                }
            }
        }
    }

    /**
     * 追加比例尺级别，分辨率级别，切片范围。
     */
    public boolean append(double[] toAppendScales, double[] toAppendResolutions, Rectangle2D toAppendBounds, String tileVersionName) {
        try {
            metaDataLock.lock();
            MetaData metaData = this.getMetaData();
            MetaData appendMetaData = metaData.appendMetaData(toAppendScales, toAppendResolutions, toAppendBounds);
            boolean versionSupported = this.tileVersionSupported();
            if (tileVersionName == null && versionSupported) {
                tileVersionName = tileVersions.getLastVersion().name;
            }
            TileVersion sourceTileVersion = null;
            if (versionSupported) {
                sourceTileVersion = tileVersions.get(tileVersionName);
                TileVersion appendedTileVersion = new TileVersion(sourceTileVersion);
                if (toAppendScales != null) {
                    appendedTileVersion.update.scaleDenominators = MathUtil.unionDoubles(sourceTileVersion.update.scaleDenominators, toAppendScales);
                }
                if (toAppendResolutions != null) {
                    appendedTileVersion.update.resolutions = MathUtil.unionDoubles(sourceTileVersion.update.resolutions, toAppendResolutions);
                }
                if (toAppendBounds != null) {
                    appendedTileVersion.update.bounds = Rectangle2D.union(new Rectangle2D[] { sourceTileVersion.update.bounds, toAppendBounds });
                }
                tileVersions.replace(tileVersionName, appendedTileVersion);
            }
            this.setMetaData((ImageMetaData) appendMetaData);
            this.upsetMetaDataToTable(appendMetaData, this.tileVersions);
            return true;
        } finally {
            metaDataLock.unlock();
        }
    }
}