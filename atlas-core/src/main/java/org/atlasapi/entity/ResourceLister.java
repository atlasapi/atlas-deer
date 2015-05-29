package org.atlasapi.entity;

import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.FluentIterable;

public interface ResourceLister<R extends Identifiable> {

    FluentIterable<R> list();
    FluentIterable<R> list(Iterable<Publisher> sources);
    
}
