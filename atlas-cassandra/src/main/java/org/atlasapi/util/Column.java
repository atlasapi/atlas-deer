package org.atlasapi.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import com.datastax.driver.core.Row;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Column<T> {

    private final String name;

    private Column(String name) {
        this.name = checkNotNull(name);
    }

    public final String name() {
        return name;
    }

    public final boolean isNullIn(Row row) {
        return row.isNull(name);
    }

    public abstract T valueFrom(Row row);

    public static Column<String> textColumn(String name) {
        return new Text(name);
    }

    public static Column<Long> bigIntColumn(String name) {
        return new BigInt(name);
    }

    public static Column<Boolean> boolColumn(String name) {
        return new Bool(name);
    }

    public static Column<ByteBuffer> bytesColumn(String name) {
        return new Bytes(name);
    }

    public static Column<java.util.Date> dateColumn(String name) {
        return new Date(name);
    }

    public static Column<java.math.BigDecimal> decimalColumn(String name) {
        return new Decimal(name);
    }

    public static Column<java.lang.Double> doubleColumn(String name) {
        return new Double(name);
    }

    public static Column<InetAddress> inetColumn(String name) {
        return new Inet(name);
    }

    public static <T> Column<java.util.List<T>> listColumn(String name, Class<T> cls) {
        return new List<T>(name, cls);
    }

    public static <K, V> Column<java.util.Map<K, V>> mapColumn(String name, Class<K> kCls,
            Class<V> vCls) {
        return new Map<K, V>(name, kCls, vCls);
    }

    public static <T> Column<java.util.Set<T>> setColumn(String name, Class<T> cls) {
        return new Set<T>(name, cls);
    }

    public static Column<java.util.UUID> uuidColumn(String name) {
        return new UUID(name);
    }

    public static Column<BigInteger> varIntColumn(String name) {
        return new VarInt(name);
    }

    private static final class Text extends Column<String> {

        public Text(String name) {
            super(name);
        }

        @Override
        public String valueFrom(Row row) {
            return row.getString(name());
        }

    }

    private static final class BigInt extends Column<Long> {

        public BigInt(String name) {
            super(name);
        }

        @Override
        public Long valueFrom(Row row) {
            return row.getLong(name());
        }

    }

    private static final class Bool extends Column<Boolean> {

        public Bool(String name) {
            super(name);
        }

        @Override
        public Boolean valueFrom(Row row) {
            return row.getBool(name());
        }

    }

    private static final class Bytes extends Column<ByteBuffer> {

        public Bytes(String name) {
            super(name);
        }

        @Override
        public ByteBuffer valueFrom(Row row) {
            return row.getBytes(name());
        }

    }

    private static final class Date extends Column<java.util.Date> {

        public Date(String name) {
            super(name);
        }

        @Override
        public java.util.Date valueFrom(Row row) {
            return row.getTimestamp(name());
        }

    }

    private static final class Decimal extends Column<BigDecimal> {

        public Decimal(String name) {
            super(name);
        }

        @Override
        public BigDecimal valueFrom(Row row) {
            return row.getDecimal(name());
        }

    }

    private static final class Double extends Column<java.lang.Double> {

        public Double(String name) {
            super(name);
        }

        @Override
        public java.lang.Double valueFrom(Row row) {
            return row.getDouble(name());
        }

    }

    private static final class Inet extends Column<InetAddress> {

        public Inet(String name) {
            super(name);
        }

        @Override
        public InetAddress valueFrom(Row row) {
            return row.getInet(name());
        }

    }

    private static final class List<T> extends Column<java.util.List<T>> {

        private Class<T> cls;

        public List(String name, Class<T> cls) {
            super(name);
            this.cls = cls;
        }

        @Override
        public java.util.List<T> valueFrom(Row row) {
            return row.getList(name(), cls);
        }

    }

    private static final class Map<K, V> extends Column<java.util.Map<K, V>> {

        private Class<K> kCls;
        private Class<V> vCls;

        public Map(String name, Class<K> kCls, Class<V> vCls) {
            super(name);
            this.kCls = kCls;
            this.vCls = vCls;
        }

        @Override
        public java.util.Map<K, V> valueFrom(Row row) {
            return row.getMap(name(), kCls, vCls);
        }

    }

    private static final class Set<T> extends Column<java.util.Set<T>> {

        private Class<T> cls;

        public Set(String name, Class<T> cls) {
            super(name);
            this.cls = cls;
        }

        @Override
        public java.util.Set<T> valueFrom(Row row) {
            return row.getSet(name(), cls);
        }

    }

    private static final class UUID extends Column<java.util.UUID> {

        public UUID(String name) {
            super(name);
        }

        @Override
        public java.util.UUID valueFrom(Row row) {
            return row.getUUID(name());
        }

    }

    private static final class VarInt extends Column<BigInteger> {

        public VarInt(String name) {
            super(name);
        }

        @Override
        public BigInteger valueFrom(Row row) {
            return row.getVarint(name());
        }

    }
}
