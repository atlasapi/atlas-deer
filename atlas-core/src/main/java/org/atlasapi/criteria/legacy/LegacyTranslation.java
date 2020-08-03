package org.atlasapi.criteria.legacy;

import javax.annotation.Nullable;

import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public class LegacyTranslation {

    private final ChildTypeMapping<?> mapping;
    private final boolean silentlyIgnore;

    private LegacyTranslation(@Nullable ChildTypeMapping<?> mapping, boolean silentlyIgnore) {
        this.mapping = mapping;
        this.silentlyIgnore = silentlyIgnore;
    }

    public static LegacyTranslation of(ChildTypeMapping<?> mapping) {
        return new LegacyTranslation(mapping, false);
    }

    public static LegacyTranslation silentlyIgnore() {
        return new LegacyTranslation(null, true);
    }

    public static LegacyTranslation unknownField() {
        return new LegacyTranslation(null, false);
    }

    public boolean hasMapping() {
        return mapping != null;
    }

    @Nullable
    public ChildTypeMapping<?> getMapping() {
        return mapping;
    }

    public boolean shouldSilentlyIgnore() {
        return silentlyIgnore;
    }

    public boolean shouldThrowException() {
        return !shouldSilentlyIgnore() && !hasMapping();
    }
}
