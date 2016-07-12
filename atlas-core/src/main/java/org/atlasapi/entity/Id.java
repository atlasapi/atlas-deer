package org.atlasapi.entity;

import java.math.BigInteger;

import org.atlasapi.hashing.Hashable;

import com.google.common.base.Function;
import com.google.common.primitives.Longs;

public final class Id implements Comparable<Id>, Hashable {

    public static final Function<Id, Long> toLongValue() {
        return ToLongValueFunction.INSTANCE;
    }

    private enum ToLongValueFunction implements Function<Id, Long> {
        INSTANCE;

        @Override
        public Long apply(Id input) {
            return input.longValue;
        }

    }

    public static final Function<Long, Id> fromLongValue() {
        return FromLongValueFunction.INSTANCE;
    }

    private enum FromLongValueFunction implements Function<Long, Id> {
        INSTANCE;

        @Override
        public Id apply(Long input) {
            return Id.valueOf(input);
        }
    }

    public static Id valueOf(String id) {
        return valueOf(Long.parseLong(id));
    }

    public static final Id valueOf(BigInteger bigInt) {
        return valueOf(bigInt.longValue());
    }

    public static final Id valueOf(int intValue) {
        return valueOf((long) intValue);
    }

    public static final Id valueOf(long longValue) {
        return new Id(longValue);
    }

    private long longValue;

    private Id(Long longValue) {
        this.longValue = longValue;
    }

    private Id(long longValue) {
        this.longValue = longValue;
    }

    public long longValue() {
        return longValue;
    }

    public BigInteger toBigInteger() {
        return BigInteger.valueOf(longValue);
    }

    @Override
    public int compareTo(Id other) {
        return Longs.compare(longValue, other.longValue);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof Id) {
            Id other = (Id) that;
            return longValue == other.longValue;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(longValue);
    }

    @Override
    public String toString() {
        return String.valueOf(longValue);
    }

}
