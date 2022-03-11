package com.supermap.services.providers;

import com.supermap.services.components.commontypes.TileType;
import com.supermap.services.tilesource.ImageTileset;
import com.supermap.services.tilesource.RemoteTileSourceAvailableListener;
import com.supermap.services.tilesource.ScopeTileSourceInfo;
import com.supermap.services.tilesource.TileSource;
import com.supermap.services.tilesource.TileSourceContainer;
import com.supermap.services.tilesource.Tileset;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;


/**
 * <p>
 * Transwarp scope地图服务提供者。
 * </p>
 * <p>
 * ScopeTileProvider 提供了从Transwarp scope切片集中获取地图服务的能力并封装了与 SuperMap iServer 地图相关的 GIS 功能。
 * </p>
 * @author ${Author}
 * @version ${Version}
 */
public class ScopeTileProvider extends TilesetMapProvider {
    private TileSource<ScopeTileSourceInfo> tileSource;

    public ScopeTileProvider() {

    };

    public ScopeTileProvider(ScopeTileProviderSetting setting) {
        super.init(setting);
    };

    @Override
    protected List<ImageTileset> initTilesets() {
        final ScopeTileProviderSetting setting = (ScopeTileProviderSetting) this.getMapProviderSetting();
        ScopeTileSourceInfo tileSourceInfo = new ScopeTileSourceInfo().serverAddresses(setting.serverAddresses).username(setting.username)
                .password(setting.password).database(setting.database);
        tileSource = TileSourceContainer.getInstance().get(tileSourceInfo, this);
        tileSource.refresh();
        tileSource.addAvailableListener(new RemoteTileSourceAvailableListener() {
            @Override
            public void onAvailableStateChanged(boolean lastState, boolean nowState) {
                if (!lastState && nowState) {
                    ScopeTileProvider.this.setMapInfoByTilesets(getTilesetsFromSource(tileSource, setting.tilesetName, setting.mapName));
                }
            }
        });
        return this.getTilesetsFromSource(tileSource, setting.tilesetName, setting.mapName);
    }

    protected List<ImageTileset> getTilesetsFromSource(TileSource<ScopeTileSourceInfo> tileSource, String tilesetName, String mapName) {
        List<ImageTileset> result = new ArrayList<ImageTileset>();
        Tileset<?, ?>[] tilesets = tileSource.getTilesets();
        for (Tileset<?, ?> tileset : tilesets) {
            if (!(tileset instanceof ImageTileset)) {
                continue;
            }
            if (StringUtils.isNotBlank(mapName) && !mapName.trim().equals(((ImageTileset) tileset).getMetaData().mapName)) {
                continue;
            }
            if (StringUtils.isNotBlank(tilesetName) && !tilesetName.trim().equals(tileset.getName())) {
                continue;
            }
            if(isMatchingType(tileset)) {
                result.add((ImageTileset) tileset);
            }
        }
        return result;
    }

    protected boolean isMatchingType(Tileset<?, ?> tileset) {
        if (tileset == null || tileset.getMetaData() == null) {
            return false;
        }
        return TileType.Image.equals(tileset.getMetaData().getTileType());
    }

    @Override
    public void dispose() {
        if (tileSource != null) {
            TileSourceContainer.getInstance().remove(tileSource, this);
        }
    }
}
