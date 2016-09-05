package org.atlasapi.entity;

import org.atlasapi.hashing.Hashable;
import org.atlasapi.media.entity.Publisher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

import static com.google.common.base.Preconditions.checkNotNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class ResourceRef implements Identifiable, Sourced, Hashable {

    protected final Id id;
    protected final Publisher source;

    @JsonCreator
    public ResourceRef(
            @JsonProperty("id") Id id,
            @JsonProperty("source") Publisher source
    ) {
        this.id = checkNotNull(id);
        this.source = checkNotNull(source);
    }

    public abstract ResourceType getResourceType();

    @JsonProperty("id")
    public Id getId() {
        return id;
    }

    @JsonProperty("source")
    public Publisher getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ResourceRef)) {
            return false;
        }
        ResourceRef that = (ResourceRef) o;
        return Objects.equal(id, that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    protected ToStringHelper toStringHelper() {
        return Objects.toStringHelper(getClass())
                .omitNullValues()
                .add("id", id)
                .add("source", source);
    }

    @Override
    public final String toString() {
        return toStringHelper().toString();
    }

}
