package org.atlasapi;

import org.atlasapi.content.ContentSearcher;
import org.atlasapi.content.ContentTitleSearcher;
import org.atlasapi.topic.PopularTopicSearcher;

public interface SearchModule {

    ContentSearcher equivContentSearcher();

    PopularTopicSearcher popularTopicSearcher();

    ContentTitleSearcher contentTitleSearcher();

}
