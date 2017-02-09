package org.atlasapi.search;

import java.util.List;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.entity.Identified;

public interface SearchResolver {

    List<Identified> search(SearchQuery query, Application sources);

}
