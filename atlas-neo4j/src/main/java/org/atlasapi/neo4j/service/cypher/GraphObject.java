package org.atlasapi.neo4j.service.cypher;

import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class GraphObject<T extends GraphObject<T>> {

    protected Optional<String> name;
    protected Optional<String> label;
    protected Map<String, String> properties;

    protected GraphObject() {
        this.name = Optional.empty();
        this.properties = Maps.newHashMap();
        this.label = Optional.empty();
    }

    public T name(String name) {
        this.name = Optional.of(name);
        return getThis();
    }

    public T label(String label) {
        this.label = Optional.of(label);
        return getThis();
    }

    public T property(String property, String value) {
        this.properties.put(
                checkNotNull(property),
                "\"" + checkNotNull(value) + "\""
        );
        return getThis();
    }

    public T property(String property, boolean value) {
        this.properties.put(
                checkNotNull(property),
                checkNotNull(String.valueOf(value))
        );
        return getThis();
    }

    public T property(String property, Number value) {
        this.properties.put(
                checkNotNull(property),
                checkNotNull(String.valueOf(value))
        );
        return getThis();
    }

    public T propertyParameter(String property, String parameter) {
        this.properties.put(
                checkNotNull(property),
                "{" + checkNotNull(parameter) + "}"
        );
        return getThis();
    }

    public abstract String build();

    protected abstract T getThis();
}
