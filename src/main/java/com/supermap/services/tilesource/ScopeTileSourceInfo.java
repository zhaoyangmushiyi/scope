package com.supermap.services.tilesource;

import com.supermap.services.components.commontypes.TileType;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * <p>
 * Transwarp scope类型的切片源连接信息。
 * </p>
 *
 */
public class ScopeTileSourceInfo extends TileSourceInfo {
    private static final long serialVersionUID = 4337511300178284696L;

    public String serverAddresses;

    /**
     * Scope数据库名。
     * <p>默认为smtiles</p>
     */
    public String database = "smtiles";

    /**
     * 用户名。
     */
    public String username;

    /**
     * 密码。
     */
    public String password;

    public ScopeTileSourceInfo() {
        super();
    }

    public ScopeTileSourceInfo(TileSourceType type) {
        super(type);
    }

    public ScopeTileSourceInfo serverAddresses(String serverAddresses) {
        if (StringUtils.isBlank(serverAddresses)) {
            throw new IllegalArgumentException("argument serverAddresses null");
        }
        this.serverAddresses = serverAddresses;
        return this;
    }

    public ScopeTileSourceInfo username(String username) {
        this.username = username;
        return this;
    }

    public ScopeTileSourceInfo password(String password) {
        this.password = password;
        return this;
    }

    public ScopeTileSourceInfo database(String database) {
        this.database = database;
        return this;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder(120000025, 120000027).append(serverAddresses).append(username).append(password).append(database);
        if (getType() != null) {
            builder.append(getType());
        }
        return builder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ScopeTileSourceInfo)) {
            return false;
        }
        ScopeTileSourceInfo rhs = (ScopeTileSourceInfo) obj;
        return new EqualsBuilder().append(getType(), rhs.getType()).append(serverAddresses, rhs.serverAddresses).append(username, rhs.username)
                .append(password, rhs.password).append(database, rhs.database).isEquals();
    }

    public TileSourceInfo copy() {
        return new ScopeTileSourceInfo().serverAddresses(this.serverAddresses).database(database).username(username).password(password);
    }

    /**
     * <p>
     * 返回切片源支持的切片类型
     * </p>
     * @return
     * @since 7.0
     */
    public TileType[] getSupportedTileTypes() {
        return new TileType[]{TileType.Image, TileType.Vector};
    }

    @Override
    public TileSourceType getType() {
        return TileSourceType.UserDefined;
    }

    @Override
    public void setType(TileSourceType type) {
        if (!(TileSourceType.UserDefined.equals(type))) {
            throw new IllegalArgumentException();
        }
        super.setType(type);

    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("(");
        sb.append("serverAddresses=");
        sb.append(ArrayUtils.toString(serverAddresses));
        sb.append(")");
        return sb.toString();
    }
}
