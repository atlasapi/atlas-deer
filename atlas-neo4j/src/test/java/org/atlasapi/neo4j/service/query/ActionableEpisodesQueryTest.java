package org.atlasapi.neo4j.service.query;

import java.util.Optional;

import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

public class ActionableEpisodesQueryTest {

    private ActionableEpisodesQuery actionableEpisodesQuery;

    @Before
    public void setUp() throws Exception {
        IndexQueryParams queryParams = new IndexQueryParams(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        "location.available", "true",
                        "broadcast.time.gt", "2016-04-01T00:00:00Z",
                        "broadcast.time.lt", "2016-04-30T00:00:00Z"
                )),
                Optional.of(Id.valueOf(20414624)) // OR 386450
        );

        actionableEpisodesQuery = ActionableEpisodesQuery.create(
                queryParams, ImmutableSet.of(
                        Publisher.BT_TVE_VOD,
                        Publisher.YOUVIEW,
                        Publisher.PA,
                        Publisher.BBC_NITRO,
                        Publisher.BBC
                )
        );
    }

    @Test
    public void parsingIso8601DateTimeWithoutMinutesDoesNotFail() throws Exception {
        IndexQueryParams queryParams = new IndexQueryParams(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                Optional.of(ImmutableMap.of(
                        "location.available", "true",
                        "broadcast.time.gt", "2016-04-01T00",
                        "broadcast.time.lt", "2016-04-30T00"
                )),
                Optional.of(Id.valueOf(20414624)) // OR 386450
        );

        ActionableEpisodesQuery.create(queryParams, ImmutableSet.of(Publisher.METABROADCAST));
    }

    @Test
    public void getQuery() throws Exception {
        String query = actionableEpisodesQuery.getQuery().getQuery();

        // TODO Test properly
        System.out.println(query);
    }
}
