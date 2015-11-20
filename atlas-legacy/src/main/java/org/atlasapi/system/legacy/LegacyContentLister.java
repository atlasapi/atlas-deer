package org.atlasapi.system.legacy;

import java.util.Iterator;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;

public interface LegacyContentLister {

    Iterator<Content> listContent(ContentListingCriteria criteria);

}
