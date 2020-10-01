package org.atlasapi.elasticsearch.content;

import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.ParentExistParameter;
import com.metabroadcast.sherlock.client.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;

public class TitleQueryBuilder {

    private static final ContentMapping CONTENT_MAPPING = IndexMapping.getContentMapping();

    // match content that has a container and whose container title matches
    // or match content which do not have a container and whose title matches
    public static BoolParameter build(String title, float weighting) {
        return SingleClauseBoolParameter.should(
                SingleClauseBoolParameter.must(
                        ParentExistParameter.exists(CONTENT_MAPPING.getContainerMapping()),
                        SearchParameter.builder()
                                .withValue(title)
                                .withMapping(CONTENT_MAPPING.getContainer().getTitle())
                                .withExactMapping(CONTENT_MAPPING.getContainer().getTitleExact())
                                .withFuzziness()
                                .withFuzzinessPrefixLength(2)
                                .withFuzzinessBoost(50F)
                                .withPhraseBoost(100F)
                                .withExactMatchBoost(200F)
                                .withBoost(weighting)
                                .build()
                ),
                SingleClauseBoolParameter.must(
                        ParentExistParameter.notExists(CONTENT_MAPPING.getContainerMapping()),
                        SearchParameter.builder()
                                .withValue(title)
                                .withMapping(CONTENT_MAPPING.getTitle())
                                .withExactMapping(CONTENT_MAPPING.getTitleExact())
                                .withFuzziness()
                                .withFuzzinessPrefixLength(2)
                                .withFuzzinessBoost(50F)
                                .withPhraseBoost(100F)
                                .withExactMatchBoost(200F)
                                .withBoost(weighting)
                                .build()
                )
        );
    }
}
