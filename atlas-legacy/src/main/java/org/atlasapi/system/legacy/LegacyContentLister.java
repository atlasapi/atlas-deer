package org.atlasapi.system.legacy;

import java.util.Iterator;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentListingProgress;

public interface LegacyContentLister {

    Iterator<Content> listContent(Iterable<Publisher> publishers, ContentListingProgress progress);

}
