package org.atlasapi.query.v5.search;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.content.ContentType;

import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;

public class SmartSearchParser {

    // Here are the basic ideas (assume case insensitivity):
    //
    //If a 4 digit number is included, content with that release year should gain weight
    //
    //If 1-3 digits are included, content with that series/episode number should gain weight,
    // with series gaining more weight.
    //
    //If the word “film” or “movie” is included, films should gain weight
    //
    //If the word “brand”, “programme”, “tv”, “tvseries”, brands should gain weight
    //
    //if the word “series”, both brands and series should gain weight as it is ambiguous,
    // and if the word “film” in included series should be ignored
    //
    //If the word “season” is included, series should gain weight
    //
    //if the word “episode” or “ep” is included, episodes shoulds gain weight
    //
    //If a known series/episode pattern is contained it should be broken down and use as above.
    // Known patterns would be S1, E1, Se1, Ep1 and their combinations, e.g. s1ep2
    //
    //For example
    // ”Westworld series” should return the tv series before the film
    // ”westworld 1973” should return the film, assuming we have the year
    // ”westworld 2” should return Series 2 first, then S2E2, then S1E2, then the film.
    // ”westworld ep1” Should return Episode 1 of both series 1 and 2.

    private final ContentMapping content = IndexMapping.getContentMapping();

    private final Pattern yearPattern =
            Pattern.compile("(?<year>(1[89]|2\\d)\\d{2})");

    private final Pattern seasonEpisodePattern =
            Pattern.compile(
                    "(^|\\s+)"
                    + "(((se?|season\\s?)(?<orSeason>\\d{1,3})|(ep?|episode\\s?)(?<orEpisode>\\d{1,3}))"
                    + "|((se?|season\\s?)(?<andSeason>\\d{1,3})(ep?|episode\\s?)(?<andEpisode>\\d{1,3}))"
                    + "|(?<either>\\d{1,3}))"
                    + "(\\s+|$)"
            );

    private final Pattern moviePattern =
            Pattern.compile("(^|\\s+)(film|movie)(\\s+|$)");

    private final Pattern brandPattern =
            Pattern.compile("(^|\\s+)(brand|program(me)?|tv\\s?(series)?)(\\s+|$)");

    private final Pattern seriesPattern =
            Pattern.compile("(^|\\s+)(series)(\\s+|$)");

    private final Pattern seasonPattern =
            Pattern.compile("(^|\\s+)(season)(\\s+|$)");

    private final Pattern episodePattern =
            Pattern.compile("(^|\\s+)(ep(isode)?)(\\s+|$)");

    private static final String FILM = ContentType.FILM.getKey();
    private static final String BRAND = ContentType.BRAND.getKey();
    private static final String SERIES = ContentType.SERIES.getKey();
    private static final String EPISODE = ContentType.EPISODE.getKey();

    public List<TermParameter<?>> parseQuery(String query) {

        String lowerQuery = query.toLowerCase();

        List<TermParameter<?>> influencers = new ArrayList<>();

        List<TermParameter<Integer>> yearInfluencers = getYearInfluencers(lowerQuery);
        influencers.addAll(yearInfluencers);

        List<TermParameter<String>> seasonEpisodeInfluencers = getSeasonEpisodeInfluencers(lowerQuery);
        influencers.addAll(seasonEpisodeInfluencers);

        boolean influenceMovies = moviePattern.matcher(lowerQuery).find();
        if (influenceMovies) {
            influencers.add(TermParameter.of(content.getType(), FILM));
        }

        boolean influenceBrands = brandPattern.matcher(lowerQuery).find();
        if (influenceBrands) {
            influencers.add(TermParameter.of(content.getType(), BRAND));
        }

        if (!influenceBrands && !influenceMovies && seriesPattern.matcher(lowerQuery).find()) {
            influencers.add(TermParameter.of(content.getType(), BRAND));
            influencers.add(TermParameter.of(content.getType(), SERIES));
        }

        if (seasonPattern.matcher(lowerQuery).find()) {
            influencers.add(TermParameter.of(content.getType(), SERIES));
        }

        if (episodePattern.matcher(lowerQuery).find()) {
            influencers.add(TermParameter.of(content.getType(), EPISODE));
        }

        return influencers;
    }

    private List<TermParameter<Integer>> getYearInfluencers(String query) {
        List<TermParameter<Integer>> influencers = new ArrayList<>();
        for (String queryPart : query.split(" ")) {
            Matcher matcher = yearPattern.matcher(queryPart);
            if (matcher.matches()) {
                Integer year = Integer.parseInt(matcher.group("year"));
                influencers.add(TermParameter.of(content.getYear(), year));
            }
        }
        return influencers;
    }

    private List<TermParameter<String>> getSeasonEpisodeInfluencers(String query) {
        List<TermParameter<String>> influencers = new ArrayList<>();
        Matcher matcher = seasonEpisodePattern.matcher(query);
        int findIndex = 0;
        while (matcher.find(findIndex)) {

            String season = matcher.group("orSeason");
            if (season == null) {
                season = matcher.group("andSeason");
            }
            if (season != null) {
                int seasonNumber = Integer.parseInt(season);
                influencers.add(TermParameter.of(
                        content.getSeriesNumber(),
                        Integer.toString(seasonNumber)));
            }

            String episode = matcher.group("orEpisode");
            if (episode == null) {
                episode = matcher.group("andEpisode");
            }
            if (episode != null) {
                int episodeNumber = Integer.parseInt(episode);
                influencers.add(TermParameter.of(
                        content.getEpisodeNumber(),
                        Integer.toString(episodeNumber)));
            }

            String either = matcher.group("either");
            if (either != null) {
                int eitherNumber = Integer.parseInt(either);
                influencers.add((TermParameter<String>) TermParameter.of(
                        content.getSeriesNumber(),
                        Integer.toString(eitherNumber)
                ).boost(1f));
                influencers.add((TermParameter<String>) TermParameter.of(
                        content.getEpisodeNumber(),
                        Integer.toString(eitherNumber)
                ).boost(0.8f));
            }

            findIndex = matcher.end() - 1; // moves matcher to before last matching space
        }
        return influencers;
    }
}
