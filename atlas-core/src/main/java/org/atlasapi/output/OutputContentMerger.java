package org.atlasapi.output;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Brand;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.Clip;
import org.atlasapi.content.Container;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.Described;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.LocalizedTitle;
import org.atlasapi.content.LocationSummary;
import org.atlasapi.content.ReleaseDate;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.Subtitles;
import org.atlasapi.content.Tag;
import org.atlasapi.entity.Award;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Rating;
import org.atlasapi.entity.Review;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class OutputContentMerger implements EquivalentsMergeStrategy<Content> {

    private static final Logger log = LoggerFactory.getLogger(OutputContentMerger.class);

    private static final long BROADCAST_START_TIME_TOLERANCE_IN_MS = Duration.standardMinutes(5)
            .getMillis();
    private static final String DO_NOT_FILL_IN_FILM_HIERARCHY_FROM_EPISODES = "do-not-fill-in-film-hierarchy-from-episodes";

    private static final Predicate<Item> HAS_BROADCASTS = input -> input.getBroadcasts()!= null && !input.getBroadcasts().isEmpty();

    private static final Predicate<Film> HAS_PEOPLE =
            film -> film.getPeople() != null && !film.getPeople().isEmpty();

    private static final Function<Described, String> TO_TITLE =
            input -> input == null ? null : input.getTitle();

    private static final Function<Described, String> TO_DESCRIPTION =
            input -> input == null ? null : input.getDescription();

    private static final Function<Described, String> TO_LONG_DESCRIPTION =
            input -> input == null ? null : input.getLongDescription();

    private static final Function<Described, String> TO_MEDIUM_DESCRIPTION =
            input -> input == null ? null : input.getMediumDescription();

    private static final Function<Described, String> TO_SHORT_DESCRIPTION =
            input -> input == null ? null : input.getShortDescription();

    private static final Function<Item, ContainerRef> TO_CONTAINER_REF =
            input -> input == null ? null : input.getContainerRef();

    private static final Function<Item, ContainerSummary> TO_CONTAINER_SUMMARY =
            input -> input == null ? null : input.getContainerSummary();

    private static final Function<Described, String> TO_PRESENTATION_CHANNEL =
            input -> input == null ? null : input.getPresentationChannel();

    private EquivalentSetContentHierarchyChooser hierarchyChooser;

    public OutputContentMerger(EquivalentSetContentHierarchyChooser hierarchyChooser) {
        this.hierarchyChooser = checkNotNull(hierarchyChooser);
    }

    //sortedEquivalents must be ordered according to source precedence
    private <T extends Described> T createMerged(Iterable<? extends T> sortedEquivalents) {

        T highestPrecedenceContent = sortedEquivalents.iterator().next();

        // First, we need to get the most specific type in the equiv set for merged
        T mostSpecificTypeContent = highestPrecedenceContent;
        for (T next : ImmutableList.copyOf(sortedEquivalents)) {
            if (!mostSpecificTypeContent.getClass().equals(next.getClass())
                    && mostSpecificTypeContent.getClass().isAssignableFrom(next.getClass())) {
                mostSpecificTypeContent = next;
            }
        }

        // Unchecked casting is safe here because we get the most specific type from the hierarchy
        T merged = (T) mostSpecificTypeContent.createNew();
        merged = mostSpecificTypeContent.copyToPreferNonNull(merged);

        // Then we need as much data as possible from the highest precedence source possible
        merged = highestPrecedenceContent.copyToPreferNonNull(merged);

        // Finally, we need the lowest id in the equiv set, in an attempt to keep the id stable
        // (this also means updating the publisher + same_as accordingly)
        T lowestIdContent = findLowestIdContent(sortedEquivalents);
        merged.setId(lowestIdContent.getId());
        merged.setPublisher(lowestIdContent.getSource());
        merged.setEquivalentTo(lowestIdContent.getEquivalentTo());

        return merged;
    }

    // return the content in the equiv set with the lowest id (to try and keep ID stable)
    private static <T extends Described> T findLowestIdContent(Iterable<T> orderedContent) {
        T lowestContent = orderedContent.iterator().next();
        for (T content : orderedContent) {
            if (lowestContent.getId().compareTo(content.getId()) > 0 )
            lowestContent = content;
        }
        return lowestContent;
    }

    @Override
    public <T extends Content> T merge(
            final Iterable<? extends T> sortedEquivalents,
            final Application application,
            Set<Annotation> activeAnnotations)
    {
        T chosen = createMerged(sortedEquivalents);
        return chosen.accept(new ContentVisitorAdapter<T>() {

            @Override
            protected T visitContainer(Container container) {
                mergeIn(application, container, filterEquivs(sortedEquivalents, Container.class), activeAnnotations);
                return uncheckedCast(container);
            }

            @Override
            protected T visitItem(Item item) {
                mergeIn(application, item, filterEquivs(sortedEquivalents, Item.class), activeAnnotations);
                return uncheckedCast(item);
            }

            @SuppressWarnings("unchecked")
            private T uncheckedCast(Content c) {
                return (T) c;
            }
        });
    }

    private <T extends Described> List<T> filterEquivs(
            Iterable<? extends Described> equivs,
            Class<T> type
    ) {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        for (Described equiv : equivs) {
            try {
                builder.add(type.cast(equiv));
            } catch (ClassCastException e) {
                log.warn(String.format(
                        "Equiv content (%s: %s) is not a %s",
                        equiv.getId(), e.getClass().getSimpleName(), type.getSimpleName()
                ), e);
            }
        }
        return builder.build();
    }

    private <T extends Described> void mergeDescribed(
            Application application,
            T chosen,
            Iterable<T> orderedContent,
            Set<Annotation> activeAnnotations
    ) {
        mergeCustomFields(chosen, orderedContent);
        mergeImages(application, chosen, orderedContent, activeAnnotations);
        chosen.setRelatedLinks(projectFieldFromEquivalents(
                orderedContent,
                Described::getRelatedLinks
        ));
        if (chosen.getTitle() == null) {
            chosen.setTitle(first(orderedContent, TO_TITLE));
        }
        mergeLocalizedTitles(chosen, orderedContent);

        if (chosen.getDescription() == null) {
            chosen.setDescription(first(orderedContent, TO_DESCRIPTION));
        }
        if (chosen.getDescription() != null
                && chosen.getDescription().equals("Concluded.")) {
            for (Described described : orderedContent) {
                //we want a better description than "Concluded."; there was an NPE happening when
                //we equiv'd to things without a description, so we add this check to avoid it
                if(Strings.isNullOrEmpty(described.getDescription())){
                    continue;
                }
                if (described.getSource().compareTo(Publisher.PA) == 0
                        && !described.getDescription().equals("Concluded.")) {
                    chosen.setDescription(described.getDescription());
                }
            }
        }
        if (chosen.getLongDescription() == null) {
            chosen.setLongDescription(first(orderedContent, TO_LONG_DESCRIPTION));
        }
        if (chosen.getMediumDescription() == null) {
            chosen.setMediumDescription(first(orderedContent, TO_MEDIUM_DESCRIPTION));
        }
        if (chosen.getShortDescription() == null) {
            chosen.setShortDescription(first(orderedContent, TO_SHORT_DESCRIPTION));
        }
        if (chosen.getPresentationChannel() == null) {
            chosen.setPresentationChannel(first(orderedContent, TO_PRESENTATION_CHANNEL));
        }

        mergeAwards(chosen, orderedContent);
    }

    private <T extends Identified> void mergeCustomFields(T chosen, Iterable<T> orderedContent) {
        for(T identified : orderedContent) {
            for(Map.Entry<String, String> customField : identified.getCustomFields().entrySet()) {
                if (!chosen.containsCustomFieldKey(customField.getKey())) {
                    chosen.addCustomField(customField.getKey(), customField.getValue());
                }
            }
        }
    }

    private <T extends Described, P> Iterable<P> projectFieldFromEquivalents(
            Iterable<T> orderedContent,
            Function<T, Iterable<P>> projector)
    {
        return Iterables.concat(StreamSupport.stream(orderedContent.spliterator(), false)
                        .map(projector::apply)
                        .collect(Collectors.toList())
                );
    }

    private <I extends Described, O> O first(Iterable<I> is,
            Function<? super I, ? extends O> transform, @Nullable O defaultValue) {
        return Iterables.getFirst(Iterables.filter(
                Iterables.transform(is, transform),
                Predicates.notNull()
        ), defaultValue);
    }

    private <I extends Described, O> O first(Iterable<I> is,
            Function<? super I, ? extends O> transform) {
        return first(is, transform, null);
    }

    private <T extends Content> void mergeContent(
            Application application,
            T chosen,
            Iterable<T> orderedContent,
            Set<Annotation> activeAnnotations
    ) {
        mergeDescribed(application, chosen, orderedContent, activeAnnotations);
        for (T content : orderedContent) {
            for (Clip clip : content.getClips()) {
                chosen.addClip(clip);
            }
        }
        mergeTags(chosen, orderedContent);
        mergeKeyPhrases(chosen, orderedContent);
        if (chosen.getYear() == null) {
            chosen.setYear(first(orderedContent, Content::getYear));
        }
        chosen.setGenres(projectFieldFromEquivalents(
                orderedContent,
                Described::getGenres
        ));
        chosen.setAliases(projectFieldFromEquivalents(
                orderedContent,
                Identified::getAliases
        ));
        mergeDuration(chosen, orderedContent);

        if (chosen instanceof Episode && ((Episode) chosen).getEpisodeNumber() == null) {
            Episode chosenEpisode = (Episode) chosen;
            MoreStreams.stream(orderedContent)
                    .filter(Episode.class::isInstance)
                    .map(Episode.class::cast)
                    .filter(e -> e.getEpisodeNumber() != null)
                    .findFirst()
                    .ifPresent(e -> {
                        chosenEpisode.setEpisodeNumber(e.getEpisodeNumber());
                        if(e.getSeriesNumber() != null) {
                            chosenEpisode.setSeriesNumber(e.getSeriesNumber());
                        }
                    });
        }

        mergeEncodings(chosen, orderedContent);

        mergeReviews(chosen, orderedContent);
        mergeRatings(chosen, orderedContent);

        chosen.setCountriesOfOrigin(projectFieldFromEquivalents(
                orderedContent,
                Content::getCountriesOfOrigin
        ));

        mergeLanguages(chosen, orderedContent);
        mergeCertificates(chosen, orderedContent);
    }

    private <T extends Content> void mergeDuration(T chosen, Iterable<T> orderedContent) {
        if(chosen instanceof Item && ((Item) chosen).getDuration() == null) {
            Item chosenItem = (Item) chosen;
            StreamSupport.stream(orderedContent.spliterator(), false)
                    .filter(Item.class::isInstance)
                    .map(Item.class::cast)
                    .filter(item -> item.getDuration() != null)
                    .findFirst()
                    .ifPresent(item -> {
                        chosenItem.setDuration(item.getDuration());
                    });
        }
    }

    private <T extends Described> void mergeLocalizedTitles(T chosen, Iterable<T> orderedContent) {

        Set<LocalizedTitle> combinedLocalizedTitles = MoreStreams.stream(orderedContent)
                .map(Described::getLocalizedTitles)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        chosen.setLocalizedTitles(combinedLocalizedTitles);
    }

    private <T extends Content> void mergeReviews(T chosen, Iterable<T> orderedContent) {

        Set<Review> combinedReviews = MoreStreams.stream(orderedContent)
                .map(Content::getReviews)
                .flatMap(review -> review.stream())
                .collect(Collectors.toSet());

        chosen.setReviews(combinedReviews);
    }

    private <T extends Content> void mergeRatings(T chosen, Iterable<T> orderedContent) {

        Set<Rating> combinedRatings = MoreStreams.stream(orderedContent)
                .map(Content::getRatings)
                .flatMap(rating -> rating.stream())
                .collect(Collectors.toSet());

        chosen.setRatings(combinedRatings);
    }

    private <T extends Described> void mergeAwards(T chosen, Iterable<T> orderedContent) {

        Set<Award> combinedAwards = MoreStreams.stream(orderedContent)
                .map(Described::getAwards)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        chosen.setAwards(combinedAwards);
    }

    private <T extends Item> void mergeIn(
            Application application,
            T chosen,
            Iterable<T> orderedContent,
            Set<Annotation> activeAnnotations
    ) {
        mergeContent(application, chosen, orderedContent, activeAnnotations);
        mergeVersions(application, chosen, orderedContent, activeAnnotations);
        boolean chosenIsFilm = chosen instanceof Film;
        mergeParentsOfItem(application, chosen, orderedContent, chosenIsFilm);

        if (chosenIsFilm) {
            mergeFilmProperties(application, (Film) chosen, Iterables.filter(orderedContent, Film.class));
        }
    }

    /**
     * Finds the first item with any hierarchy. Uses the entire hierarchy (both container and series) of an
     * episode if one is found even if not all are present (e.g. container exists but not series). This is to avoid the
     * case where the first source lists the episode as a special of a brand and thus under no series, whilst another
     * lists it as a series of a brand, since the merged result would not respect the hierarchy of the higher precedent
     * source.
     */
    private <T extends Item> void mergeParentsOfItem(
            Application application,
            T chosen,
            Iterable<T> orderedContent,
            boolean chosenIsFilm
    ) {
        if (chosen.getContainerRef() != null) {
            return;
        }
        boolean chosenIsEpisode = chosen instanceof Episode;
        boolean ignoreEpisodeHierarchies = chosenIsFilm
                && application.getAccessRoles().hasRole(DO_NOT_FILL_IN_FILM_HIERARCHY_FROM_EPISODES);
        for (T item : orderedContent) {
            // ENG-1051: Films come as episodes from C4, and RT does not want their hierarchy to mess content pages
            // of these films, so we avoid returning filling it in from episodes in the merged result
            if (ignoreEpisodeHierarchies && item instanceof Episode) {
                continue;
            }
            ContainerRef containerRef = item.getContainerRef();
            SeriesRef seriesRef = chosenIsEpisode && item instanceof Episode
                    ? ((Episode) item).getSeriesRef()
                    : null;
            if (containerRef != null || seriesRef != null) {
                chosen.setContainerRef(containerRef);
                chosen.setContainerSummary(item.getContainerSummary());
                if (chosenIsEpisode) {
                    ((Episode) chosen).setSeriesRef(seriesRef);
                }
                return;
            }
        }
    }

    private <T extends Content> void mergeKeyPhrases(T chosen, Iterable<T> orderedContent) {
        chosen.setKeyPhrases(projectFieldFromEquivalents(
                orderedContent,
                Content::getKeyPhrases
        ));
    }

    private <T extends Content> void mergeTags(T chosen, Iterable<T> orderedContent) {
        chosen.setTags(projectFieldFromEquivalents(
                orderedContent,
                input -> Iterables.transform(input.getTags(), new TagPublisherSetter(input))
        ));
    }

    private void mergeFilmProperties(Application application, Film chosen,
            Iterable<Film> orderedContent) {

        Builder<Subtitles> subtitles = ImmutableSet.<Subtitles>builder().addAll(chosen.getSubtitles());
        Builder<ReleaseDate> releases = ImmutableSet.<ReleaseDate>builder().addAll(chosen.getReleaseDates());

        for (Film film : orderedContent) {
            subtitles.addAll(film.getSubtitles());
            releases.addAll(film.getReleaseDates());
        }

        chosen.setSubtitles(subtitles.build());
        chosen.setReleaseDates(releases.build());

        if (application.getConfiguration().isPeoplePrecedenceEnabled()) {

            List<Film> topFilmMatches = application.getConfiguration()
                    .getPeopleReadPrecedenceOrdering()
                    .onResultOf(Sourceds.toPublisher())
                    .leastOf(StreamSupport.stream(orderedContent.spliterator(), false)
                            .filter(HAS_PEOPLE::apply)
                            .collect(Collectors.toList()), 1);

            if (!topFilmMatches.isEmpty()) {
                Film top = topFilmMatches.get(0);
                chosen.setPeople(top.getPeople());
            }
        }
    }

    private <T extends Described> void mergeImages(
            Application application,
            T chosen,
            Iterable<T> orderedContent,
            Set<Annotation> activeAnnotations
    ) {
        Iterable<T> orderedContentForImages = application.getConfiguration().isImagePrecedenceEnabled() ?
                application.getConfiguration()
                        .getImageReadPrecedenceOrdering()
                        .onResultOf(Sourceds.toPublisher())
                        .immutableSortedCopy(orderedContent) :
                orderedContent;

        String chosenImage = null;
        String chosenThumbnail = null;
        ImmutableSet.Builder<Image> chosenImages = ImmutableSet.builder();

        String fallbackImageIfAllUnavailable = null;
        String fallbackThumbnailIfAllUnavailable = null;

        for (T content : orderedContentForImages) {

            if (fallbackImageIfAllUnavailable == null && content.getImage() != null) {
                fallbackImageIfAllUnavailable = content.getImage();
                fallbackThumbnailIfAllUnavailable = content.getThumbnail();
            }

            boolean canUseMainImage = false;
            boolean foundMainImageInSet = false;
            Image firstAvailableImageFromSet = null;

            for (Image image : content.getImages()) {
                image.setSource(content.getSource());
                chosenImages.add(image);

                boolean isMainImage = image.getCanonicalUri().equals(content.getImage());

                if (isMainImage) {
                    foundMainImageInSet = true;
                }

                if (Image.IS_AVAILABLE_AND_NOT_GENERIC_IMAGE_CONTENT_PLAYER.apply(image)) {
                    if (isMainImage) {
                        canUseMainImage = true;
                    }
                    if (firstAvailableImageFromSet == null) {
                        firstAvailableImageFromSet = image;
                    }
                }

                if (fallbackImageIfAllUnavailable == null) {
                    fallbackImageIfAllUnavailable = image.getCanonicalUri();
                    fallbackThumbnailIfAllUnavailable = null;
                }
            }

            if (chosenImage == null) {
                // assume image can be used if we do not have its availability
                if (content.getImage() != null && (canUseMainImage || !foundMainImageInSet)) {
                    chosenImage = content.getImage();
                    chosenThumbnail = content.getThumbnail();
                } else if (firstAvailableImageFromSet != null) {
                    chosenImage = firstAvailableImageFromSet.getCanonicalUri();
                    chosenThumbnail = null;
                }
            }
        }

        if (chosenImage == null && activeAnnotations.contains(Annotation.ALL_IMAGES)) {
            chosenImage = fallbackImageIfAllUnavailable;
            chosenThumbnail = fallbackThumbnailIfAllUnavailable;
        }

        chosen.setImage(chosenImage);
        chosen.setThumbnail(chosenThumbnail);
        chosen.setImages(chosenImages.build());
    }

    private <T extends Item> void mergeVersions(Application application, T chosen,
            Iterable<T> orderedContent, Set<Annotation> activeAnnotations) {

        mergeBroadcasts(application, chosen, orderedContent, activeAnnotations);

        ImmutableList.Builder<SegmentEvent> segmentEvents = ImmutableList.builder();
        Publisher chosenPublisher = chosen.getSource();
        for (SegmentEvent segmentEvent : chosen.getSegmentEvents()) {
            segmentEvents.add(segmentEvent);
        }

        for (T item : orderedContent) {
            if (!chosenPublisher.equals(item.getSource())) {
                for (SegmentEvent segmentEvent : item.getSegmentEvents()) {
                    segmentEvents.add(segmentEvent);
                }
            }
        }
        chosen.setSegmentEvents(segmentEvents.build());
    }

    private <T extends Item> void mergeBroadcasts(
            Application application,
            T chosen,
            Iterable<T> orderedContent,
            Set<Annotation> activeAnnotations
    ) {

        // Behaviour of "broadcasts" annotation: take broadcasts from the most precedent source with
        // broadcasts, and merge them with broadcasts from less precedent sources
        // NB: source <=> publisher

        if (activeAnnotations.contains(Annotation.ALL_BROADCASTS)) {
            //return all broadcasts in the equiv set, from all sources and with no merging
            chosen.setBroadcasts(
                    MoreStreams.stream(orderedContent)
                            .map(T::getBroadcasts)
                            .flatMap(Collection::stream)
                            .collect(MoreCollectors.toImmutableSet())
            );
            return;
        }

        //TODO remove this ordering, as orderedContent is already sorted by source precedence at this point
        //first = piece of content with highest precedence source with broadcasts
        List<T> first = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .onResultOf(Sourceds.toPublisher())
                .leastOf(
                        MoreStreams.stream(orderedContent)
                                .filter(HAS_BROADCASTS::apply)
                                .collect(Collectors.toList()),
                        1
                );

        if (!first.isEmpty()) {
            if (activeAnnotations.contains(Annotation.ALL_MERGED_BROADCASTS)) {
                Set<Broadcast> finalAllMergedBroadcasts = new HashSet<>();
                Set<Broadcast> firstBroadcasts = Iterables.getOnlyElement(first).getBroadcasts();
                if(firstBroadcasts != null){
                    finalAllMergedBroadcasts.addAll(firstBroadcasts);
                }
                for (T content : orderedContent) {
                    if (content.getBroadcasts() == null) {
                        continue;
                    }
                    Set<Broadcast> broadcastsToAdd = new HashSet<>();
                    for (Broadcast broadcast : content.getBroadcasts()) {
                        boolean merged = false;
                        //merge similar broadcasts instead of adding them to set,to avoid duplicates
                        for (Broadcast alreadyAddedBroadcast : finalAllMergedBroadcasts) {
                            if (broadcastsMatch(alreadyAddedBroadcast, broadcast)) {
                                mergeBroadcast(alreadyAddedBroadcast, broadcast);
                                merged = true;
                                break;
                            }
                        }
                        if (!merged){
                            broadcastsToAdd.add(broadcast);
                        }
                    }
                    finalAllMergedBroadcasts.addAll(broadcastsToAdd);
                }
                chosen.setBroadcasts(finalAllMergedBroadcasts);
                return;
            }
            Publisher sourceForBroadcasts = Iterables.getOnlyElement(first).getSource();
            chosen.setBroadcasts(
                    MoreStreams.stream(orderedContent)
                            //filter out the broadcasts from other sources
                            .filter(item -> item.getSource().equals(sourceForBroadcasts))
                            .map(T::getBroadcasts)
                            .flatMap(Collection::stream)
                            .collect(MoreCollectors.toImmutableSet())
                );
        }

        if (chosen.getBroadcasts() != null && !chosen.getBroadcasts().isEmpty()) {
            for (Broadcast chosenBroadcast : chosen.getBroadcasts()) {
                matchAndMerge(chosenBroadcast, orderedContent);
            }
        }
    }

    private <T extends Content> void mergeEncodings(T chosen, Iterable<T> orderedContent) {
        HashSet<Encoding> encodings = Sets.newHashSet();
        if (chosen.getManifestedAs() != null) {
            encodings.addAll(chosen.getManifestedAs());
        }
        for (T item : orderedContent) {
            if (item.getManifestedAs() != null) {
                encodings.addAll(item.getManifestedAs());
            }
        }
        chosen.setManifestedAs(encodings);
    }

    private <T extends Content> void mergeCertificates(T chosen, Iterable<T> orderedContent) {
        Set<Certificate> combinedCertificates = MoreStreams.stream(orderedContent)
                .map(Content::getCertificates)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        chosen.setCertificates(combinedCertificates);
    }

    private <T extends Content> void mergeLanguages(T chosen, Iterable<T> orderedContent) {
        Set<String> combinedLanguages = MoreStreams.stream(orderedContent)
                .map(Content::getLanguages)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        chosen.setLanguages(combinedLanguages);
    }

    private <T extends Item> void matchAndMerge(final Broadcast chosenBroadcast,
            Iterable<T> orderedContent) {
        List<Broadcast> equivBroadcasts = Lists.newArrayList();
        for (T item : orderedContent) {
            Iterable<Broadcast> broadcasts = item.getBroadcasts();
            if (broadcasts != null) {
                Optional<Broadcast> matched = Iterables.tryFind(
                        broadcasts,
                        input -> broadcastsMatch(chosenBroadcast, input)
                );
                if (matched.isPresent() && matched.get() != chosenBroadcast) {
                    equivBroadcasts.add(matched.get());
                }
            }
        }
        // equivB'casts = list of matched broadcasts, ordered by precedence
        for (Broadcast equiv : equivBroadcasts) {
            mergeBroadcast(chosenBroadcast, equiv);
        }
    }

    private boolean broadcastsMatch(Broadcast first, Broadcast second) {
        return first.getChannelId().equals(second.getChannelId())
                &&
                Math.abs(
                        first.getTransmissionTime().getMillis() -
                                second.getTransmissionTime().getMillis()
                ) <= BROADCAST_START_TIME_TOLERANCE_IN_MS;
    }

    private void mergeBroadcast(Broadcast chosen, Broadcast toMerge) {
        chosen.addAliases(toMerge.getAliases());
        chosen.addAliasUrls(toMerge.getAliasUrls());

        if (chosen.getRepeat() == null && toMerge.getRepeat() != null) {
            chosen.setRepeat(toMerge.getRepeat());
        }
        if (chosen.getScheduleDate() == null && toMerge.getScheduleDate() != null) {
            chosen.setScheduleDate(toMerge.getScheduleDate());
        }
        if (chosen.getSourceId() == null && toMerge.getSourceId() != null) {
            chosen.withId(toMerge.getSourceId());
        }
        if (chosen.getSubtitled() == null && toMerge.getSubtitled() != null) {
            chosen.setSubtitled(toMerge.getSubtitled());
        }
        if (chosen.getSigned() == null && toMerge.getSigned() != null) {
            chosen.setSigned(toMerge.getSigned());
        }
        if (chosen.getAudioDescribed() == null && toMerge.getAudioDescribed() != null) {
            chosen.setAudioDescribed(toMerge.getAudioDescribed());
        }
        if (chosen.getHighDefinition() == null && toMerge.getHighDefinition() != null) {
            chosen.setHighDefinition(toMerge.getHighDefinition());
        }
        if (chosen.getWidescreen() == null && toMerge.getWidescreen() != null) {
            chosen.setWidescreen(toMerge.getWidescreen());
        }
        if (chosen.getSurround() == null && toMerge.getSurround() != null) {
            chosen.setSurround(toMerge.getSurround());
        }
        if (chosen.getLive() == null && toMerge.getLive() != null) {
            chosen.setLive(toMerge.getLive());
        }
        if (chosen.getNewSeries() == null && toMerge.getNewSeries() != null) {
            chosen.setNewSeries(toMerge.getNewSeries());
        }
        if (chosen.getNewEpisode() == null && toMerge.getNewEpisode() != null) {
            chosen.setNewEpisode(toMerge.getNewEpisode());
        }
        if (chosen.getNewOneOff() == null && toMerge.getNewOneOff() != null) {
            chosen.setNewOneOff(toMerge.getNewOneOff());
        }
        if (chosen.getPremiere() == null && toMerge.getPremiere() != null) {
            chosen.setPremiere(toMerge.getPremiere());
        }
        if (chosen.getContinuation() == null && toMerge.getContinuation() != null) {
            chosen.setContinuation(toMerge.getContinuation());
        }
        if (shouldUpdateBlackoutRestriction(chosen, toMerge)) {
            chosen.setBlackoutRestriction(toMerge.getBlackoutRestriction().get());
        }
        if (chosen.getRevisedRepeat() == null && toMerge.getRevisedRepeat() != null) {
            chosen.setRevisedRepeat(toMerge.getRevisedRepeat());
        }
    }

    private boolean shouldUpdateBlackoutRestriction(Broadcast chosen, Broadcast toMerge) {
        boolean chosenBlackedOut =
                chosen.getBlackoutRestriction().isPresent()
                        && chosen.getBlackoutRestriction().get().getAll();

        return toMerge.getBlackoutRestriction().isPresent() && !chosenBlackedOut;
    }

    private void mergeIn(
            Application application,
            Container chosen,
            Iterable<Container> orderedContent,
            Set<Annotation> activeAnnotations
    ) {
        mergeContent(application, chosen, orderedContent, activeAnnotations);
        mergeContainer(chosen, orderedContent);
    }

    private void mergeContainer(
            Container chosen,
            Iterable<Container> orderedContent
    ) {

        Iterable<Container> contentHierarchySourceOrderedContainers =
                StreamSupport.stream(orderedContent.spliterator(), false)
                .filter((Container.class)::isInstance)
                .collect(Collectors.toList());

        if (chosen.getUpcomingContent() != null && chosen.getUpcomingContent().isEmpty()) {
            java.util.Optional<Container> firstWithUpcomingContent =
                    StreamSupport.stream(contentHierarchySourceOrderedContainers.spliterator(), false)
                            .filter(container -> container.getUpcomingContent() != null
                                    && !container.getUpcomingContent().isEmpty())
                            .findFirst();
            firstWithUpcomingContent.ifPresent(container -> chosen.setUpcomingContent(container.getUpcomingContent()));
        }

        Optional<Container> first =
                hierarchyChooser.chooseBestHierarchy(contentHierarchySourceOrderedContainers);

        if (first.isPresent()) {
            Container chosenContainerForHierarchies = first.get();
            chosen.setItemRefs(chosenContainerForHierarchies.getItemRefs());
            chosen.setItemSummaries(
                    chosenContainerForHierarchies.getItemSummaries() != null
                    ? chosenContainerForHierarchies.getItemSummaries()
                    : ImmutableList.of()
            );
            if (chosen instanceof Brand && chosenContainerForHierarchies instanceof Brand) {
                Brand chosenBrand = (Brand) chosen;
                Brand precedentBrand = (Brand) chosenContainerForHierarchies;
                chosenBrand.setSeriesRefs(
                        precedentBrand.getSeriesRefs() != null ?
                        precedentBrand.getSeriesRefs() : ImmutableList.of()
                );
            }
        }

        if (chosen.getItemSummaries() == null) {
            chosen.setItemSummaries(ImmutableList.of());
        }

        if (chosen.getItemRefs() == null) {
            chosen.setItemRefs(ImmutableList.of());
        }

        if (chosen instanceof Brand && ((Brand) chosen).getSeriesRefs() == null) {
            ((Brand) chosen).setSeriesRefs(ImmutableList.of());
        }

        if (chosen instanceof Series) {
            Series chosenSeries = (Series) chosen;
            mergeParentOfSeries(chosenSeries, orderedContent);
            if (chosenSeries.getSeriesNumber() == null) {
                StreamSupport.stream(orderedContent.spliterator(), false)
                        .filter(Series.class::isInstance)
                        .map(Series.class::cast)
                        .filter(s -> s.getSeriesNumber() != null)
                        .findFirst()
                        .ifPresent(s -> {
                            chosenSeries.withSeriesNumber(s.getSeriesNumber());
                            if (s.getTotalEpisodes() != null) {
                                chosenSeries.setTotalEpisodes(s.getTotalEpisodes());
                            }
                        });
            }
        }

        Map<ItemRef, Iterable<LocationSummary>> availableContent = Maps.newHashMap();

        if (chosen.getAvailableContent() != null) {
            availableContent.putAll(chosen.getAvailableContent());
        }

        for (Container equiv : contentHierarchySourceOrderedContainers) {
            if (equiv.getAvailableContent() != null) {
                for (Map.Entry<ItemRef, Iterable<LocationSummary>> itemRefAndLocationSummary
                        : equiv.getAvailableContent().entrySet()) {
                    availableContent.putIfAbsent(
                            itemRefAndLocationSummary.getKey(),
                            itemRefAndLocationSummary.getValue()
                    );
                }
            }
        }

        chosen.setAvailableContent(availableContent);
    }

    /**
     * Takes the brandRef of the first series where it is present. This is done to support override-style content
     * which is designed not to require hierarchy if only other fields need to be overridden; such content would
     * appear like top-level series but would still need to expose the container field of its underlying series if
     * it exists.
     *
     * We used to not merge this field but have since agreed that top-level series should behave more like series than
     * brands; the null brand parent should be viewed more as 'unknown' than 'none' and thus be merged.
     */
    private <T extends Container> void mergeParentOfSeries(Series chosen, Iterable<T> orderedContent) {
        if (chosen.getBrandRef() != null) {
            return;
        }
        for (T container : orderedContent) {
            BrandRef brandRef = container instanceof Series ? ((Series) container).getBrandRef() : null;
            if (brandRef != null) {
                chosen.setBrandRef(brandRef);
                return;
            }
        }
    }

    private final static class TagPublisherSetter implements Function<Tag, Tag> {

        private final Content publishedContent;

        public TagPublisherSetter(Content publishedContent) {
            this.publishedContent = publishedContent;
        }

        @Override
        public Tag apply(Tag input) {
            input.setPublisher(publishedContent.getSource());
            return input;
        }
    }
}
