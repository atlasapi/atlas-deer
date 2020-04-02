package org.atlasapi.query.v5.search;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentTitleSearcher;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.search.SearchQuery;
import org.atlasapi.search.SearchResolver;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.sherlock.client.search.ContentSearcher;
import com.metabroadcast.sherlock.client.search.SearchHelper;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentResolvingSearcher {

    private final ContentSearcher searcher;
    private final ContentResolver contentResolver;
    private final NumberToShortStringCodec idCodec;
    private final long timeout;

    public ContentResolvingSearcher(
            ContentSearcher searcher,
            ContentResolver contentResolver,
            NumberToShortStringCodec idCodec,
            long timeout
    ) {
        this.searcher = checkNotNull(searcher);
        this.contentResolver = checkNotNull(contentResolver);
        this.idCodec = idCodec;
        this.timeout = timeout;
    }

    public List<Identified> search(SearchHelper searchHelper) {
        try {
            return Futures.transform(
                    Futures.transformAsync(
                            searcher.searchForIds(searchHelper),
                            input -> {
                                List<Id> ids = decodeIds(input);
                                if (ids.isEmpty()) {
                                    return Futures.immediateFuture(Resolved.empty());
                                } else {
                                    return contentResolver.resolveIds(ids);
                                }
                            }
                    ),
                    (Function<Resolved<Content>, List<Identified>>) input ->
                            ImmutableList.copyOf(input.getResources())
            ).get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private List<Id> decodeIds(Iterable<String> input) {
        if (input == null) {
            return ImmutableList.of();
        } else {
            return StreamSupport.stream(input.spliterator(), false)
                    .map(idCodec::decode)
                    .map(Id::valueOf)
                    .collect(Collectors.toList());
        }
    }


}
