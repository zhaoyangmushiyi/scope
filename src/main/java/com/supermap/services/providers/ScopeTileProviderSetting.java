package com.supermap.services.providers;

import com.supermap.services.components.spi.MapProviderSetting;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Transwarp scope地图瓦片服务提供者配置类。
 * @author ${Author}
 * @version ${Version}
 */
public class ScopeTileProviderSetting extends MapProviderSetting {

    /**
     * 
     */
    private static final long serialVersionUID = -180048045626324454L;
    public String serverAddresses;
    /**
     * 地图名。
     * <p>可选参数。</p>
     */
    public String mapName;

    /**
     * 切片集名。
     * <p>可选参数。 同名的地图切片大小、透明、比例尺等可能不一样，用切片集名来唯一标识一个切片集。</p>
     */
    public String tilesetName;
    /**
     * Transwarp scope数据库名。
     * <p>默认为smtiles</p>
     * @since 8.0.0
     */
    public String database = "smtiles";
    /**
     * 用户名。
     * @since 8.0.0
     */
    public String username;
    /**
     * 密码。
     * @since 8.0.0
     */
    public String password;

    @Override
    public int hashCode() {
        return new HashCodeBuilder(120000001, 120000003).append(serverAddresses).append(serverAddresses).append(mapName).append(tilesetName).append(database)
                .append(username).append(password).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ScopeTileProviderSetting)) {
            return false;
        }
        ScopeTileProviderSetting rhs = (ScopeTileProviderSetting) obj;
        return new EqualsBuilder().append(serverAddresses, rhs.serverAddresses).append(mapName, rhs.mapName).append(tilesetName, rhs.tilesetName)
                .append(username, rhs.username).append(password, rhs.password).append(database, rhs.database).isEquals();
    }
}
