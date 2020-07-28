package org.atlasapi.criteria.attribute;

import org.atlasapi.entity.Identified;

import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Attribute<T> implements QueryFactory<T> {

    protected final String name;

    private final ChildTypeMapping<?> mapping;
    private final Class<? extends Identified> target;

    protected Attribute(
            String name,
            ChildTypeMapping<?> mapping,
            Class<? extends Identified> target
    ) {
        this.name = checkNotNull(name);
        this.mapping = checkNotNull(mapping);
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

    public ChildTypeMapping<?> getMapping() {
        return mapping;
    }
}
