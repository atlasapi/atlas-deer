package org.atlasapi.content;

import com.google.common.base.MoreObjects.ToStringHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

public abstract class ContentRef extends ResourceRef {

    public ContentRef(Id id, Publisher source) {
        super(id, source);
    }

    public abstract ContentType getContentType();

    @FieldName("resource_type")
    @Override
    public final ResourceType getResourceType() {
        return ResourceType.CONTENT;
    }

    protected ToStringHelper toStringHelper() {
        return super.toStringHelper().add("type", getContentType());
    }

}
