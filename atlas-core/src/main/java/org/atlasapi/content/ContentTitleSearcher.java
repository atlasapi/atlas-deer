package org.atlasapi.content;

import org.atlasapi.search.SearchQuery;

import com.metabroadcast.sherlock.client.response.IdSearchQueryResponse;

import com.google.common.util.concurrent.ListenableFuture;

public interface ContentTitleSearcher {

    ListenableFuture<IdSearchQueryResponse> search(SearchQuery query);

}
