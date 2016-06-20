package org.atlasapi.content;

import java.util.List;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class ItemSummaryTest {

    @Test
    public void testItemSummaryOrdering() {
        ItemSummary itemSummary1 = new ItemSummary(
                new ItemRef(Id.valueOf(1), Publisher.METABROADCAST, "", DateTime.now()),
                "title1",
                "desc",
                "im",
                2012,
                ImmutableList.of(new Certificate("PG", Countries.GB))
        );
        EpisodeSummary itemSummary2 = new EpisodeSummary(
                new ItemRef(Id.valueOf(2), Publisher.METABROADCAST, "", DateTime.now()),
                "title1",
                "desc",
                "im",
                null,
                2012,
                ImmutableList.of(new Certificate("PG", Countries.GB))
        );
        EpisodeSummary itemSummary3 = new EpisodeSummary(
                new ItemRef(Id.valueOf(3), Publisher.METABROADCAST, "", DateTime.now()),
                "title1",
                "desc",
                "im",
                1,
                2012,
                ImmutableList.of(new Certificate("PG", Countries.GB))
        );
        EpisodeSummary itemSummary4 = new EpisodeSummary(
                new ItemRef(Id.valueOf(4), Publisher.METABROADCAST, "", DateTime.now()),
                "title1",
                "desc",
                "im",
                2,
                2012,
                ImmutableList.of(new Certificate("PG", Countries.GB))
        );
        EpisodeSummary itemSummary5 = new EpisodeSummary(
                new ItemRef(Id.valueOf(5), Publisher.METABROADCAST, "", DateTime.now()),
                "title1",
                "desc",
                "im",
                3,
                2012,
                ImmutableList.of(new Certificate("PG", Countries.GB))
        );

        List<ItemSummary> itemSummaries = ImmutableList.of(
                itemSummary1,
                itemSummary2,
                itemSummary3,
                itemSummary4,
                itemSummary5
        );

        for (List<ItemSummary> summaries : Collections2.permutations(itemSummaries)) {
            ImmutableList<ItemSummary> sortedSummaries = summaries
                    .stream()
                    .sorted(ItemSummary.ORDERING)
                    .collect(MoreCollectors.toImmutableList());
            assertThat(sortedSummaries.get(0), sameInstance(itemSummary3));
            assertThat(sortedSummaries.get(1), sameInstance(itemSummary4));
            assertThat(sortedSummaries.get(2), sameInstance(itemSummary5));
            assertThat(
                    sortedSummaries.subList(3, 5),
                    containsInAnyOrder(itemSummary1, itemSummary2)
            );
        }

    }
}
