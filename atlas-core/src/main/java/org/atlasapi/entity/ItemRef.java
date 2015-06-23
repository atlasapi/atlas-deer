package org.atlasapi.entity;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

public abstract class ItemRef implements Identifiable, Sourced {

    protected final Id id;
    protected final Publisher source;

    public ItemRef(Id id, Publisher source) {
        this.id = checkNotNull(id);
        this.source = checkNotNull(source);
    }
    
    public abstract ResourceType getResourceType();
    
    public Id getId() {
        return id;
    }

    public Publisher getSource() {
        return source;
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