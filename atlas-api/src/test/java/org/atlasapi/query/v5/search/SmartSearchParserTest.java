package org.atlasapi.query.v5.search;

import java.util.Arrays;
import java.util.List;

import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SmartSearchParserTest {

    private final ContentMapping content = IndexMapping.getContentMapping();
    private SmartSearchParser parser;

    @Before
    public void setUp() {
        this.parser = new SmartSearchParser();
    }

    @Test
    public void testYearInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Deadpool 2016"),
                content.getYear(), 2016);
    }

    @Test
    public void testMultipleYearsInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Deadpool 2016 2017 2018"),
                content.getYear(), 2016, 2017, 2018);
    }

    @Test
    public void testMultipleYearsSeparatedInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Deadpool 2016 Awards 2018"),
                content.getYear(), 2016, 2018);
    }

    @Test
    public void testEitherSeasonOrEpisodeInfluence() {
        List<TermParameter<?>> influencers = parser.parseQuery("Westworld 305");
        Assert.assertEquals(2, influencers.size());
        TermParameter<?> season = influencers.get(0);
        TermParameter<?> episode = influencers.get(1);
        Assert.assertEquals(content.getSeriesNumber().getFullyQualifiedName(), season.getName());
        Assert.assertEquals("305", season.getValue());
        Assert.assertEquals(content.getEpisodeNumber().getFullyQualifiedName(), episode.getName());
        Assert.assertEquals("305", episode.getValue());
        Assert.assertTrue(season.getBoost() > episode.getBoost());
    }

    @Test
    public void testFilmInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Deadpool film"),
                content.getType(), "film");
    }

    @Test
    public void testMovieInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Deadpool movie"),
                content.getType(), "film");
    }

    @Test
    public void testBrandInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld brand"),
                content.getType(), "brand");
    }

    @Test
    public void testProgramInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld program"),
                content.getType(), "brand");
    }

    @Test
    public void testProgrammeInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld programme"),
                content.getType(), "brand");
    }

    @Test
    public void testTvInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld tv"),
                content.getType(), "brand");
    }

    @Test
    public void testTvSpaceSeriesInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld tv series"),
                content.getType(), "brand");
    }

    @Test
    public void testTvSeriesInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld tvseries"),
                content.getType(), "brand");
    }

    @Test
    public void testSeriesInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld series"),
                mapVals(content.getType(), "brand"),
                mapVals(content.getType(), "series")
        );
    }

    @Test
    public void testSeasonInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld season"),
                content.getType(), "series");
    }

    @Test
    public void testSeasonNumberInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld season 1"),
                mapVals(content.getSeriesNumber(), "1"),
                mapVals(content.getType(), "series"));
    }

    @Test
    public void testEpisodeInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld episode"),
                content.getType(), "episode");
    }

    @Test
    public void testEpisodeNumberInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld episode 5"),
                mapVals(content.getEpisodeNumber(), "5"),
                mapVals(content.getType(), "episode"));
    }

    @Test
    public void testEpInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld ep"),
                content.getType(), "episode");
    }

    @Test
    public void testSeasonSNumberInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld S1"),
                content.getSeriesNumber(), "1");
    }

    @Test
    public void testSeasonSeNumberInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld Se1"),
                content.getSeriesNumber(), "1");
    }

    @Test
    public void testSeasonNumberPaddedInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld S01"),
                content.getSeriesNumber(), "1");
    }

    @Test
    public void testSeasonNumberMaxPaddedInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld S001"),
                content.getSeriesNumber(), "1");
    }

    @Test
    public void testEpisodeENumberInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld E1"),
                content.getEpisodeNumber(), "1");
    }

    @Test
    public void testEpisodeEpNumberInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld Ep1"),
                content.getEpisodeNumber(), "1");
    }

    @Test
    public void testEpisodeNumberPaddedInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld E01"),
                content.getEpisodeNumber(), "1");
    }

    @Test
    public void testEpisodeNumberMaxPaddedInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld E001"),
                content.getEpisodeNumber(), "1");
    }

    @Test
    public void testSeasonEpisodeNumberInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld S03E05"),
                mapVals(content.getSeriesNumber(), "3"),
                mapVals(content.getEpisodeNumber(), "5"));
    }

    @Test
    public void testMultipleSeasonEpisodeNumbersInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld S03E05 S2 E9 E10 S5E7"),
                mapVals(content.getSeriesNumber(), "3"),
                mapVals(content.getEpisodeNumber(), "5"),
                mapVals(content.getSeriesNumber(), "2"),
                mapVals(content.getEpisodeNumber(), "9", "10"),
                mapVals(content.getSeriesNumber(), "5"),
                mapVals(content.getEpisodeNumber(), "7"));
    }

    @Test
    public void testSeasonEpisodeNumbersInfluence() {
        Assert.assertEquals(0, parser.parseQuery("Westworld S03E05E06").size());
    }

    @Test
    public void testMultipleSeasonNumbersInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld S03 S5 S009"),
                content.getSeriesNumber(), "3", "5", "9");
    }

    @Test
    public void testMultipleEpisodeNumbersInfluence() {
        assertEqualInfluencers(
                parser.parseQuery("Westworld E03 E5 E019"),
                content.getEpisodeNumber(), "3", "5", "19");
    }

    @SafeVarargs
    private final <T> void assertEqualInfluencers(
            List<TermParameter<?>> influencers,
            ChildTypeMapping<T> mapping,
            T... values
    ) {
        Assert.assertEquals(values.length, influencers.size());
        for (int i = 0; i < influencers.size(); i++) {
            TermParameter<?> influencer = influencers.get(i);
            Assert.assertEquals(mapping.getFullyQualifiedName(), influencer.getName());
            Assert.assertEquals(values[i], influencer.getValue());
        }
    }

    @SafeVarargs
    private final <T> void assertEqualInfluencers(
            List<TermParameter<?>> influencers,
            MappingAndValues<T>... mappingAndValuesArray
    ) {
        int totalValues = Arrays.stream(mappingAndValuesArray)
                .map(m -> m.values.length)
                .reduce(0, Integer::sum);
        Assert.assertEquals(totalValues, influencers.size());
        int i = 0;
        for (MappingAndValues<T> mappingAndValues : mappingAndValuesArray) {
            int valueCount = mappingAndValues.values.length;
            assertEqualInfluencers(
                    influencers.subList(i, i+valueCount),
                    mappingAndValues.mapping, mappingAndValues.values);
            i += valueCount;
        }
    }

    @SafeVarargs
    private final <T> MappingAndValues<T> mapVals(ChildTypeMapping<T> mapping, T... values) {
        return new MappingAndValues<>(mapping, values);
    }

    class MappingAndValues<T> {
        private final ChildTypeMapping<T> mapping;
        private final T[] values;
        public MappingAndValues(ChildTypeMapping<T> mapping, T[] values) {
            this.mapping = mapping;
            this.values = values;
        }
    }
}
