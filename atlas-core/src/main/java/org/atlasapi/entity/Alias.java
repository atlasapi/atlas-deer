package org.atlasapi.entity;

import com.google.common.base.MoreObjects;
import org.atlasapi.hashing.Hashable;
import org.atlasapi.meta.annotations.FieldName;

import static com.google.common.base.Preconditions.checkNotNull;

public class Alias implements Hashable {

    public static final String URI_NAMESPACE = "uri";

    private final String namespace;
    private final String value;

    public Alias(String namespace, String value) {
        this.namespace = checkNotNull(namespace);
        this.value = checkNotNull(value);
    }

    @FieldName("namespace")
    public String getNamespace() {
        return namespace;
    }

    @FieldName("value")
    public String getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return namespace.hashCode() ^ value.hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof Alias) {
            Alias other = (Alias) that;
            return namespace.equals(other.namespace)
                    && value.equals(other.value);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("namespace", namespace)
                .add("value", value)
                .toString();
    }
}
