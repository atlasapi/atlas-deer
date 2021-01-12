package org.atlasapi.channel;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelEquivRef extends ResourceRef {

    private final String uri;

    private ChannelEquivRef(Id id, String uri, Publisher publisher) {
        super(id, publisher);
        this.uri = checkNotNull(uri);
    }

    public static ChannelEquivRef create(Id id, String uri, Publisher publisher) {
        return new ChannelEquivRef(id, uri, publisher);
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.CHANNEL;
    }

    @JsonProperty("uri")
    public String getUri() {
        return uri;
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper().add("uri", uri);
    }

}
