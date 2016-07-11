package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.hashing.Hashable;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Objects;

public class ContentGroupRef implements Hashable {

    private Id id;
    private String uri;

    public ContentGroupRef(Id id, String uri) {
        this.id = id;
        this.uri = uri;
    }

    public ContentGroupRef(ContentGroup contentGroup) {
        this.id = contentGroup.getId();
        this.uri = contentGroup.getCanonicalUri();
    }

    private ContentGroupRef() {
    }

    public void setId(Id id) {
        this.id = id;
    }

    @FieldName("id")
    public Id getId() {
        return id;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @FieldName("uri")
    public String getUri() {
        return uri;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, uri);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ContentGroupRef) {
            ContentGroupRef other = (ContentGroupRef) that;
            return Objects.equal(id, other.id);
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("Ref content group %s", id);
    }
}
