package org.atlasapi.system.legacy;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.intl.Country;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.hamcrest.core.Is;
import org.joda.time.LocalDate;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class LegacyChannelGroupTransformerTest {

    private LegacyChannelGroupTransformer objectUnderTest = new LegacyChannelGroupTransformer();


    @Test
    public void testApplyPlatform() throws Exception {
        Publisher publisher = Publisher.AMAZON_UK;
        Long id = 1L;
        Set<Country> availableCountries = ImmutableSet.of(mock(Country.class), mock(Country.class));
        final Long region1Id = 2L;
        final Long region2Id = 3L;
        final String channel1Number = "11";
        final Long channel1Id = 1L;
        final LocalDate startDate1 = new LocalDate("2014-01-01");
        final LocalDate endDate1 = new LocalDate("2014-01-02");

        final String channel2Number = "22";
        final Long channel2Id = 2L;
        final LocalDate startDate2 = new LocalDate("2014-01-03");
        final LocalDate endDate2 = new LocalDate("2014-01-04");

        Set<org.atlasapi.media.channel.ChannelNumbering> channelNumberings = ImmutableSet.of(
                org.atlasapi.media.channel.ChannelNumbering.builder()
                        .withChannel(channel1Id)
                        .withChannelNumber(channel1Number)
                        .withChannelGroup(id)
                        .withStartDate(startDate1)
                        .withEndDate(endDate1)
                        .build()
                ,
                org.atlasapi.media.channel.ChannelNumbering.builder()
                        .withChannel(channel2Id)
                        .withChannelNumber(channel2Number)
                        .withChannelGroup(id)
                        .withStartDate(startDate2)
                        .withEndDate(endDate2)
                        .build()
        );

        org.atlasapi.media.channel.Platform legacyPlatform = new org.atlasapi.media.channel.Platform();
        legacyPlatform.setPublisher(publisher);
        legacyPlatform.setId(id);
        legacyPlatform.addRegion(region1Id);
        legacyPlatform.addRegion(region2Id);
        legacyPlatform.setChannelNumberings(channelNumberings);
        legacyPlatform.setPublisher(publisher);
        legacyPlatform.setAvailableCountries(availableCountries);
        legacyPlatform.setAliases(
                ImmutableSet.of(
                        new org.atlasapi.media.entity.Alias("namespace1", "value1"),
                        new org.atlasapi.media.entity.Alias("namespace2", "value2")
                )
        );
        ChannelGroup transformed = this.objectUnderTest.apply(legacyPlatform);

        assertThat(transformed.getId().longValue(), is(id));
        assertThat(transformed.getSource(), is(publisher));
        assertThat(
                Iterables.any(
                        ((Platform) transformed).getRegions(),
                        new Predicate<ChannelGroupRef>() {
                            @Override
                            public boolean apply(ChannelGroupRef input) {
                                return input.getId().longValue() == region1Id;
                            }
                        }

                ),
                is(true)
        );

        assertThat(
                Iterables.any(
                        ((Platform) transformed).getRegions(),
                        new Predicate<ChannelGroupRef>() {
                            @Override
                            public boolean apply(ChannelGroupRef input) {
                                return input.getId().longValue() == region2Id;
                            }
                        }

                ),
                is(true)
        );

        assertThat(
                Iterables.any(
                        ((Platform) transformed).getChannels(),
                        input -> input.getChannelNumber().get().equals(channel1Number)
                                && input.getChannel().getId().longValue() == channel1Id
                                && input.getStartDate().get() == startDate1
                                && input.getEndDate().get() == endDate1

                ),
                is(true)
        );

        assertThat(
                Iterables.any(
                        ((Platform) transformed).getChannels(),
                        new Predicate<ChannelNumbering>() {
                            @Override
                            public boolean apply(ChannelNumbering input) {
                                return input.getChannelNumber().get().equals(channel2Number)
                                        && input.getChannel().getId().longValue() == channel2Id
                                        && input.getStartDate().get() == startDate2
                                        && input.getEndDate().get() == endDate2;

                            }
                        }

                ),
                is(true)
        );

        assertThat(transformed.getAvailableCountries(), Is.<Set>is(availableCountries));
        assertThat(
                transformed.getAliases(),
                is(
                        ImmutableSet.of(
                                new Alias("namespace1", "value1"),
                                new Alias("namespace2", "value2")
                        )
                )
        );
    }




    @Test
    public void testApplyRegion() throws Exception {
        Publisher publisher = Publisher.AMAZON_UK;
        Long id = 1L;
        Set<Country> availableCountries = ImmutableSet.of(mock(Country.class), mock(Country.class));
        final Long platformId = 2L;
        final String channel1Number = "11";
        final Long channel1Id = 1L;
        final LocalDate startDate1 = new LocalDate("2014-01-01");
        final LocalDate endDate1 = new LocalDate("2014-01-02");

        final String channel2Number = "22";
        final Long channel2Id = 2L;
        final LocalDate startDate2 = new LocalDate("2014-01-03");
        final LocalDate endDate2 = new LocalDate("2014-01-04");

        Set<org.atlasapi.media.channel.ChannelNumbering> channelNumberings = ImmutableSet.of(
                org.atlasapi.media.channel.ChannelNumbering.builder()
                        .withChannel(channel1Id)
                        .withChannelNumber(channel1Number)
                        .withChannelGroup(id)
                        .withStartDate(startDate1)
                        .withEndDate(endDate1)
                        .build()
                ,
                org.atlasapi.media.channel.ChannelNumbering.builder()
                        .withChannel(channel2Id)
                        .withChannelNumber(channel2Number)
                        .withChannelGroup(id)
                        .withStartDate(startDate2)
                        .withEndDate(endDate2)
                        .build()
        );

        org.atlasapi.media.channel.Region legacyRegion = new org.atlasapi.media.channel.Region();
        legacyRegion.setPublisher(publisher);
        legacyRegion.setId(id);
        legacyRegion.setPlatform(platformId);
        legacyRegion.setChannelNumberings(channelNumberings);
        legacyRegion.setPublisher(publisher);
        legacyRegion.setAvailableCountries(availableCountries);
        legacyRegion.setAliases(
                ImmutableSet.of(
                        new org.atlasapi.media.entity.Alias("namespace1", "value1"),
                        new org.atlasapi.media.entity.Alias("namespace2", "value2")
                )
        );
        ChannelGroup transformed = this.objectUnderTest.apply(legacyRegion);

        assertThat(transformed.getId().longValue(), is(id));
        assertThat(transformed.getSource(), is(publisher));
        assertThat(((Region) transformed).getPlatform().getId().longValue(), is(platformId));

        assertThat(
                Iterables.any(
                        ((Region) transformed).getChannels(),
                        new Predicate<ChannelNumbering>() {
                            @Override
                            public boolean apply(ChannelNumbering input) {
                                return input.getChannelNumber().get().equals(channel1Number)
                                        && input.getChannel().getId().longValue() == channel1Id
                                        && input.getStartDate().get() == startDate1
                                        && input.getEndDate().get() == endDate1;

                            }
                        }

                ),
                is(true)
        );

        assertThat(
                Iterables.any(
                        ((Region) transformed).getChannels(),
                        new Predicate<ChannelNumbering>() {
                            @Override
                            public boolean apply(ChannelNumbering input) {
                                return input.getChannelNumber().get().equals(channel2Number)
                                        && input.getChannel().getId().longValue() == channel2Id
                                        && input.getStartDate().get() == startDate2
                                        && input.getEndDate().get() == endDate2;

                            }
                        }

                ),
                is(true)
        );

        assertThat(transformed.getAvailableCountries(), Is.<Set>is(availableCountries));
        assertThat(
                transformed.getAliases(),
                is(
                        ImmutableSet.of(
                                new Alias("namespace1", "value1"),
                                new Alias("namespace2", "value2")
                        )
                )
        );
    }
}