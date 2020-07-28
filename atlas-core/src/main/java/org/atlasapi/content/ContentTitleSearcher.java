package org.atlasapi.content;

import org.atlasapi.search.SearchQuery;

import com.metabroadcast.sherlock.client.search.SearchQueryResponse;

import com.google.common.util.concurrent.ListenableFuture;

public interface ContentTitleSearcher {

    ListenableFuture<SearchQueryResponse> search(SearchQuery query);

}
