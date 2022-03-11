package com.supermap.services.tilesource.impl;

import com.alibaba.fastjson.JSONObject;
import com.supermap.services.components.spi.MVTTileset;
import com.supermap.services.tilesource.MVTTileMetaData;
import com.supermap.services.util.CoordinateConversionTool;
import com.supermap.services.util.TileTool;
import com.supermap.services.util.Tool;
import io.transwarp.shiva2.metadata.ShivaTableMeta;

import java.util.HashMap;
import java.util.Map;

//定义了该规范对应的切片集。该类根据标准中定义的格式，实现了瓦片的存储与管理。
public class ScopeMVTTileset extends ScopeTileset implements MVTTileset {
    private static final double DEFAULTDPI = 96.0;
    protected static final String STYLES_COLLECTION_PRIFIX = "styles";
    protected static final String FONTS_COLLECTION_PRIFIX = "fonts";
    private static final String sourcesStr = "sources";
    private static final String DEFAULT_CHARSET = "utf-8";
    private double dpi;
    private JSONObject mvtStyleJsonObj;
    private Map<String, String> spriteJsons = new HashMap<String, String>();
    private Map<String, byte[]> spriteResources = new HashMap<String, byte[]>();
    private ShivaTableMeta fontTable;

    public ScopeMVTTileset(String tilesetName, ScopeHelper scopeHelper, ShivaTableMeta metadataTable) {
        super(tilesetName, scopeHelper, metadataTable);
    }

    public ScopeMVTTileset(String name, ShivaTableMeta metadataTable) {
        super(name, metadataTable);
    }


    @Override
    public boolean init(ShivaTableMeta metadataTable) {
        if (!super.init(metadataTable)) {
            return false;
        }
//        initMvtStyles();
//        initSprites();
//        initFonts();
//        initDpi();

        boolean hasProjectionExtent = this.metaData instanceof MVTTileMetaData && ((MVTTileMetaData) this.metaData).indexBounds != null;
        if (!hasProjectionExtent) {
            try {
                ((MVTTileMetaData) this.metaData).indexBounds = CoordinateConversionTool.getEnvelope(this.metaData.prjCoordSys.epsgCode);
            } catch (Exception e) {
            }
        }
        return true;
    }


    @Override
    public JSONObject getMVTStyle(String s) {
        return null;
    }

    @Override
    public String[] listMVTSprites() {
        return new String[0];
    }

    @Override
    public byte[] getSDFFonts(String s, String s1) {
        return new byte[0];
    }

    @Override
    public String getMVTSpriteJson(String s) {
        return null;
    }

    @Override
    public byte[] getMVTSpriteResource(String s) {
        return new byte[0];
    }

    @Override
    public Double getResolutionByLevel(int i) {
        return null;
    }

    @Override
    public double getDpi() {
        return this.dpi;
    }

    private void initDpi() {
        this.dpi = TileTool.getDpiByScaleAndResolution(1.0 / metaData.scaleDenominators[0], metaData.resolutions[0],
                metaData.toMapParameter().coordUnit);
        if (Tool.equal(DEFAULTDPI, dpi, 1E-1)) {
            this.dpi = DEFAULTDPI;
        }
    }
    private void initFonts() {
        fontTable = this.scopeHelper.getTable(FONTS_COLLECTION_PRIFIX);
//        DBCursor cursor = fontsCollection.find();
//        while (cursor.hasNext()) {
//            DBObject fontDBObj = cursor.next();
//            fontDBObj.removeField("_id");
//            SDFFont font = new SDFFont();
//            font.fontName = (String) fontDBObj.get("fontName");
//            font.fileName = (String) fontDBObj.get("fileName");
//            font.fontData = (byte[]) fontDBObj.get("fontData");
//            fonts.add(font);
//        }
    }
    private void initMvtStyles() {
        String collectionName = STYLES_COLLECTION_PRIFIX + "_" + name;
        ShivaTableMeta stylesTable = this.scopeHelper.getTable(collectionName);
        /**
         *	1、这里不使用"name"字段查询style，是因为name存储的是切片集原地图的地图名，而不是切片名称，而且切片集可能存在多个style而name字段相同的情况，
         *	因此改用版本来查询style，一个style对应一个版本 by gongyx
         *	2、当同一个切片集有多个版本时，取最新一个版本（ps:其实取哪一个版本都欠妥，最好开给用户，让用户选）
         */
//        DBObject query = new BasicDBObject().append("version", this.getTileVersions().getLastVersion().name);
//        DBObject stylesDBObj = stylesTable.findOne(query);
//        if (stylesDBObj == null) {
//            return;
//        }
//        stylesDBObj.removeField("_id");
//        byte[] style = (byte[]) stylesDBObj.get("style");
//        String styleStr;
//        try {
//            styleStr = new String(style, DEFAULT_CHARSET);
//            mvtStyleJsonObj = JSON.parseObject(styleStr);
//            MVTTilesetUtil.correctStyle((JSONObject) mvtStyleJsonObj.get(sourcesStr), getName(),
//                    this.metaData.tileWidth, this.metaData.tileHeight);
//        } catch (UnsupportedEncodingException e) {
//            logger.warn(e.getMessage());
//        }
    }
}