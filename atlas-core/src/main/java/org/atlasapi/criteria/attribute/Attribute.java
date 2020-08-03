package org.atlasapi.criteria.attribute;

import javax.annotation.Nullable;

import org.atlasapi.entity.Identified;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Attribute<T> implements QueryFactory<T> {

    protected final String name;

    private final String javaAttributeName;
    private final Class<? extends Identified> target;

    protected Attribute(
            String name,
            Class<? extends Identified> target
    ) {
        this.name = checkNotNull(name);
        this.javaAttributeName = null;
        this.target = target;
    }

    protected Attribute(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target
    ) {
        this.name = checkNotNull(name);
        this.javaAttributeName = javaAttributeName;
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
    public String javaAttributeName() {
        return javaAttributeName;
    }
}
