/**
 *
 */
package com.supermap.services.providers;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.supermap.services.components.commontypes.KeywordsQueryParameterSet;
import com.supermap.services.components.commontypes.MVTStyle;
import com.supermap.services.components.commontypes.MapCapability;
import com.supermap.services.components.commontypes.MapParameter;
import com.supermap.services.components.commontypes.OutputFormat;
import com.supermap.services.components.commontypes.QueryResult;
import com.supermap.services.components.commontypes.Rectangle2D;
import com.supermap.services.components.commontypes.TileOutputType;
import com.supermap.services.components.commontypes.TileType;
import com.supermap.services.components.commontypes.VectorStyle;
import com.supermap.services.components.commontypes.VectorStyleParameter;
import com.supermap.services.components.commontypes.VectorStyleType;
import com.supermap.services.components.commontypes.VectorTileData;
import com.supermap.services.components.commontypes.VectorTileParameter;
import com.supermap.services.components.spi.MVTTileset;
import com.supermap.services.components.spi.NotSupportedException;
import com.supermap.services.components.spi.TiledVectorProvider;
import com.supermap.services.tilesource.ImageMetaData;
import com.supermap.services.tilesource.ImageTileInfo;
import com.supermap.services.tilesource.ImageTileset;
import com.supermap.services.tilesource.MVTTileMetaData;
import com.supermap.services.tilesource.Tile;
import com.supermap.services.tilesource.Tileset;
import com.supermap.services.tilesource.TilesetInfo;
import com.supermap.services.tilesource.impl.ScopeMVTTileset;
import com.supermap.services.util.TileTool;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;


/**
 * <p>
 * Transwarp scope Mvt服务提供者。
 * </p>
 *
 * @author ${Author}
 * @version ${Version}
 * @since 9.1.0
 *
 */
public class ScopeMVTTileProvider extends ScopeTileProvider implements TiledVectorProvider {
    private List<ImageTileset> tilesets = new ArrayList<ImageTileset>();

    public ScopeMVTTileProvider() {
    }

    public ScopeMVTTileProvider(ScopeTileProviderSetting setting) {
        super.init(setting);
    }

    /**
     * @param tilesets
     * @since 9.1.0
     */
    @Override
    protected void setMapInfoByTilesets(List<ImageTileset> tilesets) {
        super.setMapInfoByTilesets(tilesets);
        this.tilesets.addAll(tilesets);
    }

    @Override
    public VectorStyle getVectorStyle(VectorStyleParameter vectorStyleParameter) {
        if (VectorStyleType.MapBox_GL == vectorStyleParameter.type) {
            JSONObject mvtStyleJsonObject = getMVTStyleJson(vectorStyleParameter.mapName);
            JSONObject sourcesJsonObj = (JSONObject) mvtStyleJsonObject.get("sources");
            for (String mapName : sourcesJsonObj.keySet()) {
                Object mapSource = sourcesJsonObj.get(mapName);
                if (mapSource == null) {
                    continue;
                }
                JSONObject mapJsonObj = (JSONObject) mapSource;
                JSONArray tilesJsonArray = new JSONArray();
                JSONArray tilesJsonArrayObj = (JSONArray) mapJsonObj.get("tiles");
                for (int i = 0; i < tilesJsonArrayObj.size(); i++) {
                    String tilesJson = MVTTileProviderUtil.replaceTileJson(tilesJsonArrayObj.getString(i),
                            vectorStyleParameter.tileURLTemplate);
                    tilesJsonArray.add(i, tilesJson);
                }
                mapJsonObj.put("tiles", tilesJsonArray);
            }
            return new MVTStyle(mvtStyleJsonObject);
        }
        throw new NotSupportedException();
    }

    private JSONObject getMVTStyleJson(String mapName) {
        MVTTileset tileset = getTilesetByMapName(mapName);
        if (tileset != null) {
            return tileset.getMVTStyle(mapName);
        }
        return null;
    }

    @Override
    public boolean support(String mapName, MapCapability capability) {
        TilesetInfo[] infos = this.getTilesetInfos(mapName);
        for (TilesetInfo info : infos) {
            if (info.metaData instanceof ImageMetaData) {
                ImageMetaData imageMetaData = (ImageMetaData) (info.metaData);
                if (TileType.Vector.equals(imageMetaData.getTileType())
                        && OutputFormat.MVT.equals(imageMetaData.tileFormat)) {
                    if (MapCapability.MVTCapabilities.equals(capability) || MapCapability.MBStyle.equals(capability)) {
                        return true;
                    }
                } else {
                    if (MapCapability.MapImage.equals(capability)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public QueryResult queryByKeywords(String mapName, KeywordsQueryParameterSet queryParameterSet) {
        throw new NotSupportedException();
    }

    @Override
    public VectorTileData getVectorTile(VectorTileParameter vectorTileParameter) {
        throw new NotSupportedException();
    }

    @Override
    public void updateVectorStyle(String layerName, VectorStyleType type, String style) {
        throw new NotSupportedException();
    }

    @Override
    public byte[] getSymbolData(String symbolId, OutputFormat format) {
        throw new NotSupportedException();
    }

    @Override
    public byte[] getMVTTile(VectorTileParameter vectorTileParameter) {
        Tile tile = null;
        TileOutputType tileOutputType = getMVTTileOutputType(vectorTileParameter);
        ScopeMVTTileset tileset = getTilesetByMapName(vectorTileParameter.name);
        switch (tileOutputType) {
            case ZXY:
                tile = new Tile(Long.valueOf(vectorTileParameter.x), Long.valueOf(vectorTileParameter.y),
                        tileset.getResolutionByLevel(Integer.valueOf(vectorTileParameter.z)), null);
                break;
            case ScaleXY:
                tile = new Tile(Long.valueOf(vectorTileParameter.x), Long.valueOf(vectorTileParameter.y),
                        TileTool.scaleToResolution(vectorTileParameter.scale, tileset.getDpi(),
                                vectorTileParameter.prjCoordSys.coordUnit),
                        null);
                break;
            case ViewBounds:
                tile = tileset.getMetaData().tile(vectorTileParameter.viewBounds, vectorTileParameter.viewer, null);
                break;
            default:
        }
        if (tile == null || tile.x < 0 || tile.y < 0) {
            return null;
        }
        ImageTileInfo info = tileset.get(tile);
        if (info != null && info.tileData != null) {
            return info.tileData;
        }
        return new byte[0];
    }

    private TileOutputType getMVTTileOutputType(VectorTileParameter vectorTileParameter) {
        if (!StringUtils.isBlank(vectorTileParameter.z)) {
            return TileOutputType.ZXY;
        }
        if (Double.valueOf(vectorTileParameter.scale) > 0 && !StringUtils.isBlank(vectorTileParameter.x)
                && !StringUtils.isBlank(vectorTileParameter.y)) {
            return TileOutputType.ScaleXY;
        }
        return TileOutputType.ViewBounds;
    }

    @Override
    public String[] listMVTSprites(MapParameter mapParameter) {
        if (this.getNames().isEmpty()) {
            return null;
        }
        List<String> names = new ArrayList<String>();
        for (String mapName : this.getNames()) {
            MVTTileset tileset = getTilesetByMapName(mapName);
            if (tileset != null) {
                return tileset.listMVTSprites();
            }
        }
        return names.toArray(new String[names.size()]);
    }

    @Override
    public String getMVTSpriteJson(MapParameter mapParameter, String spriteName) {
        if (mapParameter == null || StringUtils.isBlank(spriteName)) {
            return null;
        }
        MVTTileset tileset = getTilesetByMapName(mapParameter.name);
        if (tileset != null) {
            return tileset.getMVTSpriteJson(spriteName);
        }
        return null;
    }

    @Override
    public byte[] getMVTSpriteResource(MapParameter mapParameter, String spriteName) {
        MVTTileset tileset = getTilesetByMapName(mapParameter.name);
        if (tileset != null) {
            return tileset.getMVTSpriteResource(spriteName);
        }
        return null;
    }

    /**
     *
     * @param fontstack
     * @param range
     * @return
     * @since 9.1.0
     */
    @Override
    public byte[] getSDFFonts(String fontstack, String range) {
        for (String mapName : this.getNames()) {
            MVTTileset tileset = getTilesetByMapName(mapName);
            if (tileset != null) {
                return tileset.getSDFFonts(fontstack, range);
            }
        }
        return null;
    }

    private ScopeMVTTileset getTilesetByMapName(String mapName) {
        if (StringUtils.isBlank(mapName)) {
            return null;
        }
        for (ImageTileset tileset : tilesets) {
            if (tileset.getMetaData().mapName.equals(mapName) && (tileset instanceof MVTTileset)) {
                return (ScopeMVTTileset) tileset;
            }
        }
        return null;
    }

    @Override
    public Rectangle2D getProjectionExtent(String mapName) {
        Rectangle2D rectangle2D = new Rectangle2D();
        ScopeMVTTileset tileset = getTilesetByMapName(mapName);
        ImageMetaData imageMetaData = tileset.getMetaData();
        if ((imageMetaData instanceof MVTTileMetaData) && ((MVTTileMetaData) imageMetaData).indexBounds != null) {
            return ((MVTTileMetaData) imageMetaData).indexBounds;
        }
        return rectangle2D;
    }

    @Override
    protected boolean isMatchingType(Tileset<?, ?> tileset) {
        if (tileset == null || tileset.getMetaData() == null) {
            return false;
        }
        return TileType.Vector.equals(tileset.getMetaData().getTileType());
    }

}
