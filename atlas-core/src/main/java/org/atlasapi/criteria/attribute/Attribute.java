package org.atlasapi.criteria.attribute;

import javax.annotation.Nullable;

import org.atlasapi.entity.Identified;

import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Attribute<T> implements QueryFactory<T> {

    protected final String name;

    private final ChildTypeMapping<?> directMapping;
    private final Class<? extends Identified> target;

    protected Attribute(
            String name,
            @Nullable ChildTypeMapping<?> directMapping,
            Class<? extends Identified> target
    ) {
        this.name = checkNotNull(name);
        this.directMapping = directMapping;
        this.target = target;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Attribute<?>) {
            Attribute<?> attribute = (Attribute<?>) obj;
            return name.equals(attribute.name) && target.equals(attribute.target);
        }
        return false;
    }

    public String externalName() {
        return name;
    }

    @Nullable
    public ChildTypeMapping<?> getDirectMapping() {
        return directMapping;
    }
}
