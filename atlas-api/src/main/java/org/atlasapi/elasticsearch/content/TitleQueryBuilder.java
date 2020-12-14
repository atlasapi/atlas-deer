package org.atlasapi.elasticsearch.content;

import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.CompositeTitleSearchParameter;
import com.metabroadcast.sherlock.client.parameter.ExistParameter;
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
                        ExistParameter.exists(CONTENT_MAPPING.getContainer().getId()),
                        CompositeTitleSearchParameter.forContainerTitle(title)
                                .withRelativeDefaultBoosts(weighting)
                                .build()
                ),
                SingleClauseBoolParameter.must(
                        ExistParameter.notExists(CONTENT_MAPPING.getContainer().getId()),
                        CompositeTitleSearchParameter.forContentTitle(title)
                                .withRelativeDefaultBoosts(weighting)
                                .build()
                )
        );
    }
}
