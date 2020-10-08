package org.atlasapi.elasticsearch.content;

import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.ParentExistParameter;
import com.metabroadcast.sherlock.client.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;

public class TitleQueryBuilder {

    private static final ContentMapping CONTENT_MAPPING = IndexMapping.getContentMapping();

    private static final Integer FUZZINESS_PREFIX_LENGTH = 2;
    private static final Float FUZZINESS_BOOST = 50f;
    private static final Float PHRASE_BOOST = 100f;
    private static final Float EXACT_MATCH_BOOST = 200f;
    private static final Float PREFIX_BOOST = 75f;

    // match content that has a container and whose container title matches
    // or match content that does not have a container and whose title matches
    public static BoolParameter build(String title, float weighting) {
        return SingleClauseBoolParameter.should(
                SingleClauseBoolParameter.must(
                        ParentExistParameter.exists(CONTENT_MAPPING.getContainerMapping()),
                        SearchParameter.builder()
                                .withValue(title)
                                .withMapping(CONTENT_MAPPING.getContainer().getTitle())
                                .withExactMapping(CONTENT_MAPPING.getContainer().getTitleExact())
                                .withFuzziness()
                                .withFuzzinessPrefixLength(FUZZINESS_PREFIX_LENGTH)
                                .withFuzzinessBoost(FUZZINESS_BOOST)
                                .withPhraseBoost(PHRASE_BOOST)
                                .withExactMatchBoost(EXACT_MATCH_BOOST)
                                .withPrefixOnExactMappingBoost(PREFIX_BOOST)
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
                                .withFuzzinessPrefixLength(FUZZINESS_PREFIX_LENGTH)
                                .withFuzzinessBoost(FUZZINESS_BOOST)
                                .withPhraseBoost(PHRASE_BOOST)
                                .withExactMatchBoost(EXACT_MATCH_BOOST)
                                .withPrefixOnExactMappingBoost(PREFIX_BOOST)
                                .withBoost(weighting)
                                .build()
                )
        );
    }
}
