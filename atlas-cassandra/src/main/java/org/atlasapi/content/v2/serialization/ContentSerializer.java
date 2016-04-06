package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.Content;

public interface ContentSerializer {

    org.atlasapi.content.v2.model.Content serialize(Content content);
}
