package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.Content;

public interface ContentSerializer {

    Iterable<org.atlasapi.content.v2.model.Content> serialize(Content content);
}
