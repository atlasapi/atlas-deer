package org.atlasapi.query.v4.search.support;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentTitleSearcher;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.search.SearchQuery;
import org.atlasapi.search.SearchResolver;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentResolvingSearcher implements SearchResolver {

    private final ContentTitleSearcher searcher;
    private final ContentResolver contentResolver;
    private final long timeout;

    public ContentResolvingSearcher(ContentTitleSearcher searcher, ContentResolver contentResolver,
            long timeout) {
        this.searcher = checkNotNull(searcher);
        this.contentResolver = checkNotNull(contentResolver);
        this.timeout = timeout;
    }

    @Override
    public List<Identified> search(SearchQuery query, Application application) {
        try {

            return Futures.transform(
                    Futures.transformAsync(
                        searcher.search(query),
                        input -> {
                            if (input.getIds().isEmpty()) {
                                return Futures.immediateFuture(Resolved.empty());
                            }
                            return contentResolver.resolveIds(input.getIds());
                        }
                    ),
                    (Function<Resolved<Content>, List<Identified>>) input ->
                            ImmutableList.copyOf(input.getResources())
            ).get(timeout, TimeUnit.MILLISECONDS);

        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
