package org.atlasapi.hashing.content;

import org.atlasapi.content.Content;

public interface ContentHasher {

    String hash(Content content);

}
