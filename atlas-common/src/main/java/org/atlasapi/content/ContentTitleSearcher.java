package org.atlasapi.content;

import org.atlasapi.search.SearchQuery;
import org.atlasapi.search.SearchResults;

import com.google.common.util.concurrent.ListenableFuture;

public interface ContentTitleSearcher {

    ListenableFuture<SearchResults> search(SearchQuery query);

}
