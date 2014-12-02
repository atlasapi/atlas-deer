package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

public final class BrandRef extends ContainerRef {

    public BrandRef(Id id, Publisher source) {
        super(id, source);
    }

    @FieldName("content_type")
    @Override
    public ContentType getContentType() {
        return ContentType.BRAND;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof BrandRef) {
            BrandRef other = (BrandRef) that;
            return id.equals(other.id)
                && source.equals(other.source);
        }
        return false;
    }

}
