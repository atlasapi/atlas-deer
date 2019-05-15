package org.atlasapi.output;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.Clip;
import org.atlasapi.content.Container;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.Described;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.LocationSummary;
import org.atlasapi.content.ReleaseDate;
import org.atlasapi.content.Series;
import org.atlasapi.content.Subtitles;
import org.atlasapi.content.Tag;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Person;
import org.atlasapi.entity.Rating;
import org.atlasapi.entity.Review;
import org.atlasapi.entity.Sourced;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class OutputContentMerger implements EquivalentsMergeStrategy<Content> {

    private static final Logger log = LoggerFactory.getLogger(OutputContentMerger.class);

    private static final long BROADCAST_START_TIME_TOLERANCE_IN_MS = Duration.standardMinutes(5)
            .getMillis();

    private static final Predicate<Described> HAS_AVAILABLE_AND_NOT_GENERIC_IMAGE_CONTENT_PLAYER_SET
            = content -> content.getImage()!= null &&
                    isImageAvailableAndNotGenericImageContentPlayer(
                            content.getImage(),
                            content.getImages()
                    );

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

    private EquivalentSetContentHierarchyChooser hierarchyChooser;

    public OutputContentMerger(EquivalentSetContentHierarchyChooser hierarchyChooser) {
        this.hierarchyChooser = checkNotNull(hierarchyChooser);
    }

    @Deprecated
    public <T extends Described> List<T> merge(Application application, List<T> contents,
            Set<Annotation> activeAnnotations) {
        Ordering<Sourced> publisherComparator = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .onResultOf(Sourceds.toPublisher());

        List<T> merged = Lists.newArrayListWithCapacity(contents.size());
        Set<T> processed = Sets.newHashSet();

        for (T content : contents) {
            if (processed.contains(content)) {
                continue;
            }
            List<T> same = publisherComparator.sortedCopy(findSame(content, contents));
            processed.addAll(same);

            Map.Entry<T, List<T>> chosenAndRest = getChosenAndRest(same);
            T chosen = chosenAndRest.getKey();
            List<T> notChosen = chosenAndRest.getValue();

            chosen = mergeIdAndEquivTo(chosen);

            // defend against broken transitive equivalence
            if (merged.contains(chosen)) {
                continue;
            }

            if (chosen instanceof Container) {
                mergeIn(application, (Container) chosen, filterEquivs(notChosen, Container.class));
            }
            if (chosen instanceof Item) {
                mergeIn(application, (Item) chosen, filterEquivs(notChosen, Item.class), activeAnnotations);
            }
            if (chosen instanceof ContentGroup) {
                mergeIn(application, (ContentGroup) chosen, filterEquivs(notChosen, ContentGroup.class));
            }
            merged.add(chosen);
        }
        return merged;
    }

    private <T extends Described> Map.Entry<T, List<T>> getChosenAndRest(
            List<? extends T> orderedSame
    ) {
        List<T> rest = ImmutableList.copyOf(orderedSame.subList(1, orderedSame.size()));
        return new SimpleImmutableEntry<>(
                createChosen(orderedSame.get(0), rest),
                rest
        );
    }

    private <T extends Described> T createChosen(
            T chosen,
            Iterable<? extends T> rest
    ) {
        // We need to get the most specific type in the equiv set
        T best = chosen;
        for (T next : ImmutableSet.copyOf(rest)) {
            if (!best.getClass().equals(next.getClass())
                    && best.getClass().isAssignableFrom(next.getClass())) {
                best = next;
            }
        }
        // But then we need as many settings as possible from the most precedent source.
        // But without overwriting with nulls
        chosen.copyToPreferNonNull(best);
        return best;
    }

    private static final Ordering<EquivalenceRef> EQUIV_ID_ORDERING =
            Ordering.from(Comparator.comparing(EquivalenceRef::getId)).nullsLast();
    private <T extends Described> EquivalenceRef lowestEquivRef(T chosen) {
        EquivalenceRef lowest = EquivalenceRef.valueOf(chosen);
        for (EquivalenceRef ref : chosen.getEquivalentTo()) {
            lowest = EQUIV_ID_ORDERING.min(lowest, ref);
        }
        return lowest;
    }

    private <T extends Described> T mergeIdAndEquivTo(T chosen) {
        EquivalenceRef chosenRef = EquivalenceRef.valueOf(chosen);
        EquivalenceRef lowestRef = lowestEquivRef(chosen);
        if (chosenRef.equals(lowestRef)) {
            return chosen;
        }
        Set<EquivalenceRef> equivs = chosen.getEquivalentTo();
        Set<EquivalenceRef> newEquivs = new LinkedHashSet<>(equivs.size() + 1);
        newEquivs.add(chosenRef);
        newEquivs.addAll(equivs);
        newEquivs.remove(lowestRef);
        if (equivs.size() != newEquivs.size()) {
            log.warn("Problem merging equivs: OLD{id: {}, equivTo: {}} NEW{id: {}, equivTo: {}}",
                    chosenRef, equivs, lowestRef, newEquivs);
        }
        chosen.setId(lowestRef.getId());
        chosen.setPublisher(lowestRef.getSource());
        chosen.setEquivalentTo(newEquivs);
        return chosen;
    }

    @Override
    public <T extends Content> T merge(T chosen, final Iterable<? extends T> equivalents,
            final Application application,
            Set<Annotation> activeAnnotations) {
        chosen = createChosen(chosen, equivalents);
        chosen = mergeIdAndEquivTo(chosen);
        return chosen.accept(new ContentVisitorAdapter<T>() {

            @Override
            protected T visitContainer(Container container) {
                mergeIn(application, container, filterEquivs(equivalents, Container.class));
                return uncheckedCast(container);
            }

            @Override
            protected T visitItem(Item item) {
                mergeIn(application, item, filterEquivs(equivalents, Item.class), activeAnnotations);
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

    private <T extends Described> List<T> findSame(T content, Iterable<T> contents) {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        builder.add(content);
        for (T possiblyEquivalent : contents) {
            if (!content.equals(possiblyEquivalent) && possiblyEquivalent.isEquivalentTo(content)) {
                builder.add(possiblyEquivalent);
            }
        }
        return builder.build();
    }

    private <T extends ContentGroup> void mergeIn(Application application, T chosen,
            Iterable<T> notChosen) {
        mergeDescribed(application, chosen, notChosen);
        for (ContentGroup contentGroup : notChosen) {
            for (ContentRef ref : contentGroup.getContents()) {
                chosen.addContent(ref);
            }
        }
        if (chosen instanceof Person) {
            Person person = (Person) chosen;
            ImmutableSet.Builder<String> quotes = ImmutableSet.builder();
            quotes.addAll(person.getQuotes());
            for (Person unchosen : Iterables.filter(notChosen, Person.class)) {
                quotes.addAll(unchosen.getQuotes());
                person.withName(person.name() != null ? person.name() : unchosen.name());
                person.setGivenName(person.getGivenName() != null
                                    ? person.getGivenName()
                                    : unchosen.getGivenName());
                person.setFamilyName(person.getFamilyName() != null
                                     ? person.getFamilyName()
                                     : unchosen.getFamilyName());
                person.setGender(person.getGender() != null
                                 ? person.getGender()
                                 : unchosen.getGender());
                person.setBirthDate(person.getBirthDate() != null
                                    ? person.getBirthDate()
                                    : unchosen.getBirthDate());
                person.setBirthPlace(person.getBirthPlace() != null
                                     ? person.getBirthPlace()
                                     : unchosen.getBirthPlace());
            }
            person.setQuotes(quotes.build());
        }
    }

    private <T extends Described> void mergeDescribed(Application application, T chosen,
            Iterable<T> notChosen) {
        mergeCustomFields(chosen, notChosen);
        applyImagePrefs(application, chosen, notChosen);
        chosen.setRelatedLinks(projectFieldFromEquivalents(
                chosen,
                notChosen,
                Described::getRelatedLinks
        ));
        if (chosen.getTitle() == null) {
            chosen.setTitle(first(notChosen, TO_TITLE));
        }
        if (chosen.getDescription() == null) {
            chosen.setDescription(first(notChosen, TO_DESCRIPTION));
        }
        if (chosen.getDescription() != null
                && chosen.getDescription().equals("Concluded.")) {
            for (Described notChosenDescribed : notChosen) {
                //we want a better description than "Concluded."; there was an NPE happening when
                //we equiv'd to things without a description, so we add this check to avoid it
                if(Strings.isNullOrEmpty(notChosenDescribed.getDescription())){
                    continue;
                }
                if (notChosenDescribed.getSource().compareTo(Publisher.PA) == 0
                        && !notChosenDescribed.getDescription().equals("Concluded.")) {
                    chosen.setDescription(notChosenDescribed.getDescription());
                }
            }
        }
        if (chosen.getLongDescription() == null) {
            chosen.setLongDescription(first(notChosen, TO_LONG_DESCRIPTION));
        }
        if (chosen.getMediumDescription() == null) {
            chosen.setMediumDescription(first(notChosen, TO_MEDIUM_DESCRIPTION));
        }
        if (chosen.getShortDescription() == null) {
            chosen.setShortDescription(first(notChosen, TO_SHORT_DESCRIPTION));
        }
    }

    private <T extends Identified> void mergeCustomFields(
            T chosen,
            Iterable<T> notChosen
    ) {
        for(T identified : notChosen) {
            for(Map.Entry<String, String> customField : identified.getCustomFields().entrySet()) {
                if (!chosen.containsCustomFieldKey(customField.getKey())) {
                    chosen.addCustomField(customField.getKey(), customField.getValue());
                }
            }
        }
    }

    private <T extends Described, P> Iterable<P> projectFieldFromEquivalents(T chosen,
            Iterable<T> notChosen, Function<T, Iterable<P>> projector) {
        return Iterables.concat(
                projector.apply(chosen),
                Iterables.concat(StreamSupport.stream(notChosen.spliterator(), false)
                        .map(projector::apply)
                        .collect(Collectors.toList())
                )
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

    private <T extends Content> void mergeContent(Application application, T chosen,
            Iterable<T> notChosen) {
        mergeDescribed(application, chosen, notChosen);
        for (T notChosenItem : notChosen) {
            for (Clip clip : notChosenItem.getClips()) {
                chosen.addClip(clip);
            }
        }
        mergeTags(chosen, notChosen);
        mergeKeyPhrases(chosen, notChosen);
        if (chosen.getYear() == null) {
            chosen.setYear(first(notChosen, Content::getYear));
        }
        chosen.setGenres(projectFieldFromEquivalents(
                chosen,
                notChosen,
                Described::getGenres
        ));
        chosen.setAliases(projectFieldFromEquivalents(
                chosen,
                notChosen,
                Identified::getAliases
        ));

        if (chosen instanceof Episode && ((Episode) chosen).getEpisodeNumber() == null) {
            Episode chosenEpisode = (Episode) chosen;
            StreamSupport.stream(notChosen.spliterator(), false)
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

        mergeEncodings(application, chosen, notChosen);

        mergeReviews(chosen, notChosen);
        mergeRatings(chosen, notChosen);

        chosen.setCountriesOfOrigin(projectFieldFromEquivalents(
                chosen,
                notChosen,
                Content::getCountriesOfOrigin
        ));
    }

    private <T extends Content> void mergeReviews(T chosen, Iterable<T> notChosen) {

        List<T> allContent = new ImmutableList.Builder<T>()
                .add(chosen)
                .addAll(notChosen)
                .build();

        Set<Review> combinedReviews = allContent.stream()
                .map(Content::getReviews)
                .flatMap(review -> review.stream())
                .collect(Collectors.toSet());

        chosen.setReviews(combinedReviews);
    }

    private <T extends Content> void mergeRatings(T chosen, Iterable<T> notChosen) {

        List<T> allContent = new ImmutableList.Builder<T>()
                .add(chosen)
                .addAll(notChosen)
                .build();

        Set<Rating> combinedRatings = allContent.stream()
                .map(Content::getRatings)
                .flatMap(rating -> rating.stream())
                .collect(Collectors.toSet());

        chosen.setRatings(combinedRatings);
    }

    private <T extends Item> void mergeIn(Application application, T chosen,
            Iterable<T> notChosen,
            Set<Annotation> activeAnnotations) {
        mergeContent(application, chosen, notChosen);
        mergeVersions(application, chosen, notChosen, activeAnnotations);

        if (chosen instanceof Film) {
            mergeFilmProperties(application, (Film) chosen, Iterables.filter(notChosen, Film.class));
        }
        if (chosen.getContainerRef() == null) {
            chosen.setContainerRef(first(notChosen, TO_CONTAINER_REF));
        }
        if (chosen.getContainerSummary() == null) {
            chosen.setContainerSummary(first(notChosen, TO_CONTAINER_SUMMARY));
        }
    }

    private <T extends Content> void mergeKeyPhrases(T chosen, Iterable<T> notChosen) {
        chosen.setKeyPhrases(projectFieldFromEquivalents(
                chosen,
                notChosen,
                Content::getKeyPhrases
        ));
    }

    private <T extends Content> void mergeTags(T chosen, Iterable<T> notChosen) {
        chosen.setTags(projectFieldFromEquivalents(
                chosen,
                notChosen,
                input -> Iterables.transform(input.getTags(), new TagPublisherSetter(input))
        ));
    }

    private void mergeFilmProperties(Application application, Film chosen,
            Iterable<Film> notChosen) {
        Builder<Subtitles> subtitles = ImmutableSet.<Subtitles>builder().addAll(chosen.getSubtitles());
        Builder<String> languages = ImmutableSet.<String>builder().addAll(chosen.getLanguages());
        Builder<Certificate> certs = ImmutableSet.<Certificate>builder().addAll(chosen.getCertificates());
        Builder<ReleaseDate> releases = ImmutableSet.<ReleaseDate>builder().addAll(chosen.getReleaseDates());

        for (Film film : notChosen) {
            subtitles.addAll(film.getSubtitles());
            languages.addAll(film.getLanguages());
            certs.addAll(film.getCertificates());
            releases.addAll(film.getReleaseDates());
        }

        chosen.setSubtitles(subtitles.build());
        chosen.setLanguages(languages.build());
        chosen.setCertificates(certs.build());
        chosen.setReleaseDates(releases.build());

        if (application.getConfiguration().isPeoplePrecedenceEnabled()) {
            Iterable<Film> all = Iterables.concat(ImmutableList.of(chosen), notChosen);
            List<Film> topFilmMatches = application.getConfiguration()
                    .getPeopleReadPrecedenceOrdering()
                    .onResultOf(Sourceds.toPublisher())
                    .leastOf(Iterables.filter(all, HAS_PEOPLE), 1);

            if (!topFilmMatches.isEmpty()) {
                Film top = topFilmMatches.get(0);
                chosen.setPeople(top.getPeople());
            }
        }
    }

    private <T extends Described> void applyImagePrefs(Application application, T chosen,
            Iterable<T> notChosen) {
        Iterable<T> all = Iterables.concat(ImmutableList.of(chosen), notChosen);
        if (application.getConfiguration().isImagePrecedenceEnabled()) {

            List<T> topImageMatches = application.getConfiguration()
                    .getImageReadPrecedenceOrdering()
                    .onResultOf(Sourceds.toPublisher())
                    .leastOf(
                            Iterables.filter(
                                    all,
                                    HAS_AVAILABLE_AND_NOT_GENERIC_IMAGE_CONTENT_PLAYER_SET
                            ),
                            1
                    );

            if (!topImageMatches.isEmpty()) {
                T top = topImageMatches.get(0);
                top.getImages().forEach(img -> img.setSource(top.getSource()));
                chosen.setImages(top.getImages());
                chosen.setImage(top.getImage());
                chosen.setThumbnail(top.getThumbnail());

            } else {
                chosen.setImages(Iterables.filter(
                        chosen.getImages(),
                        img -> isImageAvailableAndNotGenericImageContentPlayer(img)
                ));
                chosen.getImages().forEach(img -> img.setSource(chosen.getSource()));
                chosen.setImage(null);
            }
        } else {
            chosen.setImages(projectFieldFromEquivalents(
                    chosen,
                    notChosen,
                    input -> {
                        input.getImages().forEach(img -> img.setSource(input.getSource()));
                        return input.getImages();
                    }
            ));
        }
    }

    private <T extends Item> void mergeVersions(Application application, T chosen,
            Iterable<T> notChosen, Set<Annotation> activeAnnotations) {
        mergeBroadcasts(application, chosen, notChosen, activeAnnotations);
        List<T> notChosenOrdered = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .onResultOf(Sourceds.toPublisher())
                .sortedCopy(notChosen);

        ImmutableList.Builder<SegmentEvent> segmentEvents = ImmutableList.builder();
        Publisher chosenPublisher = chosen.getSource();
        for (SegmentEvent segmentEvent : chosen.getSegmentEvents()) {
            segmentEvents.add(segmentEvent);
        }

        for (T notChosenItem : notChosenOrdered) {
            if (!chosenPublisher.equals(notChosenItem.getSource())) {
                for (SegmentEvent segmentEvent : notChosenItem.getSegmentEvents()) {
                    segmentEvents.add(segmentEvent);
                }
            }
        }
        chosen.setSegmentEvents(segmentEvents.build());
    }

    private <T extends Item> void mergeBroadcasts(
            Application application,
            T chosen,
            Iterable<T> notChosen,
            Set<Annotation> activeAnnotations
    ) {

        // Behaviour of "broadcasts" annotation: take broadcasts from the most precedent source with
        // broadcasts, and merge them with broadcasts from less precedent sources
        // NB: source <=> publisher

        List<T> all = ImmutableList.<T>builder().add(chosen).addAll(notChosen).build();

        if (activeAnnotations.contains(Annotation.ALL_BROADCASTS)) {
            //return all broadcasts in the equiv set, from all sources and with no merging
            chosen.setBroadcasts(
                    all.stream()
                            .map(T::getBroadcasts)
                            .flatMap(Collection::stream)
                            .collect(MoreCollectors.toImmutableSet())
            );
            return;
        }

        List<T> notChosenOrdered = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .onResultOf(Sourceds.toPublisher())
                .sortedCopy(notChosen);

        //first = piece of content with highest precedence source with broadcasts
        List<T> first = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .onResultOf(Sourceds.toPublisher())
                .leastOf(
                        all.stream()
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
                for (T content : notChosenOrdered) {
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
                    all.stream()
                            //filter out the broadcasts from other sources
                            .filter(item -> item.getSource().equals(sourceForBroadcasts))
                            .map(T::getBroadcasts)
                            .flatMap(Collection::stream)
                            .collect(MoreCollectors.toImmutableSet())
                );
        }

        if (chosen.getBroadcasts() != null && !chosen.getBroadcasts().isEmpty()) {
            for (Broadcast chosenBroadcast : chosen.getBroadcasts()) {
                matchAndMerge(chosenBroadcast, notChosenOrdered);
            }
        }
    }

    private static Predicate<Item> isPublisher(Publisher publisher) {
        return input -> publisher.equals(input.getSource());
    }

    private <T extends Content> void mergeEncodings(Application application, T chosen,
            Iterable<T> notChosen) {
        List<T> notChosenOrdered = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .onResultOf(Sourceds.toPublisher())
                .sortedCopy(notChosen);

        HashSet<Encoding> encodings = Sets.newHashSet();
        if (chosen.getManifestedAs() != null) {
            encodings.addAll(chosen.getManifestedAs());
        }
        for (T notChosenItem : notChosenOrdered) {
            if (notChosenItem.getManifestedAs() != null) {
                encodings.addAll(notChosenItem.getManifestedAs());
            }
        }
        chosen.setManifestedAs(encodings);
    }

    private <T extends Item> void matchAndMerge(final Broadcast chosenBroadcast,
            List<T> notChosen) {
        List<Broadcast> equivBroadcasts = Lists.newArrayList();
        for (T notChosenItem : notChosen) {
            Iterable<Broadcast> notChosenBroadcasts = notChosenItem.getBroadcasts();
            if (notChosenBroadcasts != null) {
                Optional<Broadcast> matched = Iterables.tryFind(
                        notChosenBroadcasts,
                        input -> broadcastsMatch(chosenBroadcast, input)
                );
                if (matched.isPresent()) {
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

    private static boolean isImageAvailableAndNotGenericImageContentPlayer(
            String imageUri,
            Iterable<Image> images
    ) {

        // Fneh. Image URIs differ between the image attribute and the canonical URI on Images.
        // See PaProgrammeProcessor for why.
        String rewrittenUri = imageUri.replace(
                "http://images.atlasapi.org/pa/",
                "http://images.atlas.metabroadcast.com/pressassociation.com/"
        );

        // If there is a corresponding Image object for this URI, we check its availability and
        // whether it is generic.
        for (Image image : images) {
            if (image.getCanonicalUri().equals(rewrittenUri)) {
                return isImageAvailableAndNotGenericImageContentPlayer(image);
            }
        }
        // Otherwise, we can only assume the image is available as we know no better
        return true;
    }
    private static boolean isImageAvailableAndNotGenericImageContentPlayer(Image image) {
        return Image.IS_AVAILABLE.apply(image)
                && !Image.Type.GENERIC_IMAGE_CONTENT_PLAYER.equals(image.getType());
    }

    private void mergeIn(
            Application application,
            Container chosen,
            Iterable<Container> notChosen
    ) {
        mergeContent(application, chosen, notChosen);
        mergeContainer(chosen, notChosen);
    }

    private void mergeContainer(
            Container chosen,
            Iterable<Container> notChosen
    ) {

        Iterable<Container> orderedEquivalents;

        orderedEquivalents = Iterables.concat(ImmutableSet.of(chosen), notChosen);

        Iterable<Container> contentHierarchySourceOrderedContainers = StreamSupport.stream(
                orderedEquivalents.spliterator(), false)
                .filter((Container.class)::isInstance)
                .collect(Collectors.toList());

        if (chosen.getUpcomingContent() != null && chosen.getUpcomingContent().isEmpty()) {
            chosen.setUpcomingContent(
                    first(
                            contentHierarchySourceOrderedContainers,
                            input -> input.getUpcomingContent().isEmpty()
                                     ? null
                                     : input.getUpcomingContent(),
                            ImmutableMap.of()
                    )
            );
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

        if (chosen instanceof Series && ((Series) chosen).getSeriesNumber() == null) {
            Series chosenSeries = (Series) chosen;
            StreamSupport.stream(notChosen.spliterator(), false)
                    .filter(Series.class::isInstance)
                    .map(Series.class::cast)
                    .filter(s -> s.getSeriesNumber() != null)
                    .findFirst()
                    .ifPresent(s -> {
                        chosenSeries.withSeriesNumber(s.getSeriesNumber());
                        if(s.getTotalEpisodes() != null) {
                            chosenSeries.setTotalEpisodes(s.getTotalEpisodes());
                        }
                    });
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
