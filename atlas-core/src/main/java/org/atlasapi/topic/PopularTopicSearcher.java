package org.atlasapi.topic;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.query.Selection;

import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.Interval;

public interface PopularTopicSearcher {

    ListenableFuture<FluentIterable<Id>> popularTopics(Interval interval, Selection selection);

}
