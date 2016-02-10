package org.atlasapi.entity;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentListingProgress;

import com.google.common.collect.FluentIterable;

public interface ResourceLister<R extends Identifiable> {

    FluentIterable<R> list();

    FluentIterable<R> list(ContentListingProgress progress);

    FluentIterable<R> list(Iterable<Publisher> sources);

    FluentIterable<R> list(Iterable<Publisher> sources, ContentListingProgress progress);

}
