package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.Content;

public interface ContentDeserializer {

    Content deserialize(org.atlasapi.content.v2.model.Content internal);
}
