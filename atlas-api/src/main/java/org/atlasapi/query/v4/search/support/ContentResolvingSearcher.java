package org.atlasapi.query.v4.search.support;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentTitleSearcher;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.search.SearchQuery;
import org.atlasapi.search.SearchResolver;
import org.atlasapi.search.SearchResults;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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
    public List<Identified> search(SearchQuery query, ApplicationSources sources) {
        try {

            return Futures.transform(Futures.transform(
                    searcher.search(query),
                    new AsyncFunction<SearchResults, Resolved<Content>>() {

                        @Override
                        public ListenableFuture<Resolved<Content>> apply(SearchResults input)
                                throws Exception {
                            if (input.getIds().isEmpty()) {
                                return Futures.immediateFuture(Resolved.<Content>empty());
                            }
                            return contentResolver.resolveIds(input.getIds());
                        }
                    }
            ), new Function<Resolved<Content>, List<Identified>>() {

                @Override
                public List<Identified> apply(Resolved<Content> input) {
                    return ImmutableList.<Identified>copyOf(input.getResources());
                }
            }).get(timeout, TimeUnit.MILLISECONDS);

        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
