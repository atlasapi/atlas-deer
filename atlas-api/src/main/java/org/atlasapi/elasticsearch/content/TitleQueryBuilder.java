package org.atlasapi.elasticsearch.content;

import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.CompositeTitleSearchParameter;
import com.metabroadcast.sherlock.client.parameter.ParentExistParameter;
import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;

public class TitleQueryBuilder {

    private static final ContentMapping CONTENT_MAPPING = IndexMapping.getContentMapping();

    // match content that has a container and whose container title matches
    // or match content that does not have a container and whose title matches
    public static BoolParameter build(String title, float weighting) {
        return SingleClauseBoolParameter.should(
                SingleClauseBoolParameter.must(
                        ParentExistParameter.exists(CONTENT_MAPPING.getContainerMapping()),
                        CompositeTitleSearchParameter.forContainerTitle(title)
                                .withRelativeDefaultBoosts(weighting)
                                .build()
                ),
                SingleClauseBoolParameter.must(
                        ParentExistParameter.notExists(CONTENT_MAPPING.getContainerMapping()),
                        CompositeTitleSearchParameter.forContentTitle(title)
                                .withRelativeDefaultBoosts(weighting)
                                .build()
                )
        );
    }
}
