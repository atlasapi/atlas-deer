package org.atlasapi.content;

import java.util.List;

import org.atlasapi.util.Strings;

import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.helpers.OccurrenceClause;
import com.metabroadcast.sherlock.client.search.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.search.parameter.Parameter;
import com.metabroadcast.sherlock.client.search.parameter.PrefixParameter;
import com.metabroadcast.sherlock.client.search.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.search.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import com.metabroadcast.sherlock.common.type.TextMapping;

import com.google.common.collect.Iterables;

public class TitleQueryBuilder {

    private static final ContentMapping CONTENT_MAPPING = IndexMapping.getContent();
    private static final TextMapping TITLE_MAPPING = CONTENT_MAPPING.getTitle();
    private static final TextMapping PARENT_TITLE_MAPPING = CONTENT_MAPPING.getContainer().getTitle();

    private static final int USE_PREFIX_SEARCH_UP_TO = 2;

    public static void addTitleQueryToBuilder(
            SearchQuery.Builder searchQueryBuilder,
            String title,
            float boost
    ) {
        List<String> tokens = Strings.tokenize(title, true);

        if (shouldUsePrefixSearch(tokens)) {
            searchQueryBuilder.addSearcher(
                    PrefixParameter.of(PARENT_TITLE_MAPPING, Iterables.getOnlyElement(tokens))
                            .boost(boost)
            );
        } else {
            searchQueryBuilder.addSearcher(fuzzyTermSearch(title, tokens));
        }
    }

    private static boolean shouldUsePrefixSearch(List<String> tokens) {
        return tokens.size() == 1
                && Iterables.getOnlyElement(tokens).length() <= USE_PREFIX_SEARCH_UP_TO;
    }

    // TODO add non top-level title searching
    private static BoolParameter fuzzyTermSearch(String title, List<String> tokens) {

        SingleClauseBoolParameter.Builder parameterForAllTokens =
                SingleClauseBoolParameter.builder(OccurrenceClause.MUST);

        for (String token : tokens) {

            SingleClauseBoolParameter.Builder parameterForThisToken =
                    SingleClauseBoolParameter.builder(OccurrenceClause.SHOULD);

            parameterForThisToken.addParameter(
                    PrefixParameter.of(PARENT_TITLE_MAPPING, token)
                            .boost(50)
            );

            parameterForThisToken.addParameter(
                    SearchParameter.builder()
                            .withMapping(PARENT_TITLE_MAPPING)
                            .withValue(token)
                            .withFuzziness()
                            .withFuzzinessPrefixLength(USE_PREFIX_SEARCH_UP_TO)
                            .build()
            );

            parameterForAllTokens.addParameter(parameterForThisToken.build());
        }

        return SingleClauseBoolParameter.should(
                parameterForAllTokens.build(),
                PrefixParameter.of(PARENT_TITLE_MAPPING, title).boost(50),
                TermParameter.of(PARENT_TITLE_MAPPING, title).boost(200)
        );
    }
}
