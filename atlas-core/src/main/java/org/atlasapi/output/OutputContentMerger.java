package org.atlasapi.output;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.Clip;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentVisitorAdapter;
import org.atlasapi.content.Described;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Film;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.LocationSummary;
import org.atlasapi.content.ReleaseDate;
import org.atlasapi.content.Subtitles;
import org.atlasapi.content.Tag;
import org.atlasapi.entity.Distribution;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Person;
import org.atlasapi.entity.Rating;
import org.atlasapi.entity.Review;
import org.atlasapi.entity.Sourced;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
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

import static com.google.common.base.Preconditions.checkNotNull;

public class OutputContentMerger implements EquivalentsMergeStrategy<Content> {

    private static final long BROADCAST_START_TIME_TOLERANCE_IN_MS = Duration.standardMinutes(5)
            .getMillis();

    private static final Predicate<Described> HAS_AVAILABLE_AND_NOT_GENERIC_IMAGE_CONTENT_PLAYER_SET
            = content -> content.getImage()!= null &&
                    isImageAvailableAndNotGenericImageContentPlayer(
                            content.getImage(),
                            content.getImages()
                    );

    private static final Predicate<Item> HAS_BROADCASTS =
            input -> input.getBroadcasts()!= null && !input.getBroadcasts().isEmpty();

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

    private EquivalentSetContentHierarchyChooser hierarchyChooser;

    public OutputContentMerger(EquivalentSetContentHierarchyChooser hierarchyChooser) {
        this.hierarchyChooser = checkNotNull(hierarchyChooser);
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    public <T extends Described> List<T> merge(ApplicationSources sources, List<T> contents) {
        Ordering<Sourced> publisherComparator = sources.getSourcedReadOrdering();

        List<T> merged = Lists.newArrayListWithCapacity(contents.size());
        Set<T> processed = Sets.newHashSet();

        for (T content : contents) {
            if (processed.contains(content)) {
                continue;
            }
            List<T> same = publisherComparator.sortedCopy(findSame(content, contents));
            processed.addAll(same);

            T chosen = same.get(0);

            chosen.setId(lowestId(chosen));

            // defend against broken transitive equivalence
            if (merged.contains(chosen)) {
                continue;
            }

            List<T> notChosen = same.subList(1, same.size());

            if (chosen instanceof Container) {
                mergeIn(sources, (Container) chosen, (List<Container>) notChosen);
            }
            if (chosen instanceof Item) {
                mergeIn(sources, (Item) chosen, (List<Item>) notChosen);
            }
            if (chosen instanceof ContentGroup) {
                mergeIn(sources, (ContentGroup) chosen, (List<ContentGroup>) notChosen);
            }
            merged.add(chosen);
        }
        return merged;
    }

    private <T extends Described> Id lowestId(T chosen) {
        Id lowest = chosen.getId();
        for (EquivalenceRef ref : chosen.getEquivalentTo()) {
            Id candidate = ref.getId();
            lowest = Ordering.natural().nullsLast().min(lowest, candidate);
        }
        return lowest;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Content> T merge(T chosen, final Iterable<? extends T> equivalents,
            final ApplicationSources sources) {
        chosen.setId(lowestId(chosen));
        return chosen.accept(new ContentVisitorAdapter<T>() {

            @Override
            protected T visitContainer(Container container) {
                mergeIn(sources, container, (Iterable<Container>) equivalents);
                return (T) container;
            }

            @Override
            protected T visitItem(Item item) {
                mergeIn(sources, item, (Iterable<Item>) equivalents);
                return (T) item;
            }
        });
    }

    @SuppressWarnings("unchecked")
    private <T extends Described> List<T> findSame(T brand, Iterable<T> contents) {
        List<T> same = Lists.newArrayList(brand);
        for (T possiblyEquivalent : contents) {
            if (!brand.equals(possiblyEquivalent) && possiblyEquivalent.isEquivalentTo(brand)) {
                same.add(possiblyEquivalent);
            }
        }
        return same;
    }

    private <T extends ContentGroup> void mergeIn(ApplicationSources sources, T chosen,
            Iterable<T> notChosen) {
        mergeDescribed(sources, chosen, notChosen);
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
                person.setGivenName(
                        person.getGivenName() != null
                        ? person.getGivenName()
                        : unchosen.getGivenName()
                );
                person.setFamilyName(
                        person.getFamilyName() != null
                        ? person.getFamilyName()
                        : unchosen.getFamilyName()
                );
                person.setGender(
                        person.getGender() != null
                        ? person.getGender()
                        : unchosen.getGender()
                );
                person.setBirthDate(
                        person.getBirthDate() != null
                        ? person.getBirthDate()
                        : unchosen.getBirthDate()
                );
                person.setBirthPlace(
                        person.getBirthPlace() != null
                        ? person.getBirthPlace()
                        : unchosen.getBirthPlace()
                );
                person.setPseudoForename(
                        person.getPseudoForename() != null
                        ? person.getPseudoForename()
                        : unchosen.getPseudoForename()
                );
                person.setPseudoSurname(
                        person.getPseudoSurname() != null
                        ? person.getPseudoSurname()
                        : unchosen.getPseudoSurname()
                );
                person.setAdditionalInfo(
                        person.getAdditionalInfo() != null
                        ? person.getAdditionalInfo()
                        : unchosen.getAdditionalInfo()
                );
                person.setBilling(
                        person.getBilling() != null
                        ? person.getBilling()
                        : unchosen.getBilling()
                );
                person.setPersonSource(
                        person.getPersonSource() != null
                        ? person.getPersonSource()
                        : unchosen.getPersonSource()
                );
                person.setSourceTitle(
                        person.getSourceTitle() !=null
                        ? person.getSourceTitle()
                        : unchosen.getSourceTitle()
                );
            }
            person.setQuotes(quotes.build());
        }
    }

    private <T extends Described> void mergeDescribed(ApplicationSources sources, T chosen,
            Iterable<T> notChosen) {
        applyImagePrefs(sources, chosen, notChosen);
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

    private <T extends Described, P> Iterable<P> projectFieldFromEquivalents(T chosen,
            Iterable<T> notChosen, Function<T, Iterable<P>> projector) {
        return Iterables.concat(
                projector.apply(chosen),
                Iterables.concat(Iterables.transform(notChosen, projector))
        );
    }

    private <I extends Described, O> O first(Iterable<I> is,
            Function<? super I, ? extends O> transform, O defaultValue) {
        return Iterables.getFirst(Iterables.filter(
                Iterables.transform(is, transform),
                Predicates.notNull()
        ), defaultValue);
    }

    private <I extends Described, O> O first(Iterable<I> is,
            Function<? super I, ? extends O> transform) {
        return first(is, transform, null);
    }

    private <T extends Content> void mergeContent(ApplicationSources sources, T chosen,
            Iterable<T> notChosen) {
        mergeDescribed(sources, chosen, notChosen);
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
        mergeEncodings(sources, chosen, notChosen);

        mergeReviews(chosen, notChosen);
        mergeRatings(chosen, notChosen);
        mergeDistributons(chosen, notChosen);
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

    private <T extends Content> void mergeDistributons(T chosen, Iterable<T> notChosen) {

        List<T> allContent = new ImmutableList.Builder<T>()
                .add(chosen)
                .addAll(notChosen)
                .build();

        Set<Distribution> combinedDistributions = allContent.stream()
                .map(Content::getDistributions)
                .filter(Objects::nonNull)
                .flatMap(distributions ->
                        StreamSupport.stream(distributions.spliterator(), false)
                )
                .collect(Collectors.toSet());

        chosen.setDistributions(combinedDistributions);
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

    private <T extends Item> void mergeIn(ApplicationSources sources, T chosen,
            Iterable<T> notChosen) {
        mergeContent(sources, chosen, notChosen);
        mergeVersions(sources, chosen, notChosen);
        if (chosen instanceof Film) {
            mergeFilmProperties(sources, (Film) chosen, Iterables.filter(notChosen, Film.class));
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

    private void mergeFilmProperties(ApplicationSources sources, Film chosen,
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

        if (sources.peoplePrecedenceEnabled()) {
            Iterable<Film> all = Iterables.concat(ImmutableList.of(chosen), notChosen);
            List<Film> topFilmMatches = sources.getSourcedPeoplePrecedenceOrdering()
                    .leastOf(Iterables.filter(all, HAS_PEOPLE), 1);
            if (!topFilmMatches.isEmpty()) {
                Film top = topFilmMatches.get(0);
                chosen.setPeople(top.getPeople());
            }
        }
    }

    private <T extends Described> void applyImagePrefs(ApplicationSources sources, T chosen,
            Iterable<T> notChosen) {
        Iterable<T> all = Iterables.concat(ImmutableList.of(chosen), notChosen);
        if (sources.imagePrecedenceEnabled()) {

            List<T> topImageMatches = sources.getSourcedImagePrecedenceOrdering().leastOf(
                    Iterables.filter(all, HAS_AVAILABLE_AND_NOT_GENERIC_IMAGE_CONTENT_PLAYER_SET),
                    1
            );

            if (!topImageMatches.isEmpty()) {
                T top = topImageMatches.get(0);
                top.getImages().forEach(img -> img.setSource(top.getSource()));
                chosen.setImages(top.getImages());
                chosen.setImage(top.getImage());
                chosen.setThumbnail(top.getThumbnail());

            } else {
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

    private <T extends Item> void mergeVersions(ApplicationSources sources, T chosen,
            Iterable<T> notChosen) {
        mergeBroadcasts(sources, chosen, notChosen);
        List<T> notChosenOrdered = sources.getSourcedReadOrdering().sortedCopy(notChosen);

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

    private <T extends Item> void mergeBroadcasts(ApplicationSources sources, T chosen,
            Iterable<T> notChosen) {

        // Take broadcasts from the most precedent source with broadcasts, and
        // merge them with broadcasts from less precedent sources.

        Iterable<T> all = Iterables.concat(ImmutableList.of(chosen), notChosen);
        List<T> first = sources.getSourcedReadOrdering()
                .leastOf(Iterables.filter(all, HAS_BROADCASTS), 1);

        if (!first.isEmpty()) {
            Publisher sourceForBroadcasts = Iterables.getOnlyElement(first).getSource();
            chosen.setBroadcasts(Sets.newHashSet(
                    Iterables.concat(
                            Iterables.transform(
                                    Iterables.filter(all, isPublisher(sourceForBroadcasts)),
                                    (Function<Item, Set<Broadcast>>) Item::getBroadcasts
                            )
                    )));

        }

        List<T> notChosenOrdered = sources.getSourcedReadOrdering().sortedCopy(notChosen);
        if (chosen.getBroadcasts() != null && !chosen.getBroadcasts().isEmpty()) {
            for (Broadcast chosenBroadcast : chosen.getBroadcasts()) {
                matchAndMerge(chosenBroadcast, notChosenOrdered);
            }
        }
    }

    private static Predicate<Item> isPublisher(Publisher publisher) {
        return input -> publisher.equals(input.getSource());
    }

    private <T extends Content> void mergeEncodings(ApplicationSources sources, T chosen,
            Iterable<T> notChosen) {
        List<T> notChosenOrdered = sources.getSourcedReadOrdering().sortedCopy(notChosen);
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
        if (chosen.getPremiere() == null && toMerge.getPremiere() != null) {
            chosen.setPremiere(toMerge.getPremiere());
        }
        if (!chosen.getBlackoutRestriction().isPresent() && toMerge.getBlackoutRestriction()
                .isPresent()) {
            chosen.setBlackoutRestriction(toMerge.getBlackoutRestriction().get());
        }
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
               return Image.IS_AVAILABLE.apply(image)
                       && !Image.Type.GENERIC_IMAGE_CONTENT_PLAYER.equals(image.getType());
            }
        }
        // Otherwise, we can only assume the image is available as we know no better
        return true;
    }

    private void mergeIn(ApplicationSources sources, Container chosen,
            Iterable<Container> notChosen) {
        mergeContent(sources, chosen, notChosen);
        mergeContainer(sources, chosen, notChosen);
    }

    private void mergeContainer(ApplicationSources sources, Container chosen,
            Iterable<Container> notChosen) {

        Iterable<Container> orderedEquivalents;
        Optional<Ordering<Sourced>> sourcedContentHierarchyOrdering =
                sources.getSourcedContentHierarchyOrdering();

        if (sourcedContentHierarchyOrdering.isPresent()) {
            orderedEquivalents = sourcedContentHierarchyOrdering.get()
                    .sortedCopy(Iterables.concat(ImmutableSet.of(chosen), notChosen));
        } else {
            orderedEquivalents = Iterables.concat(ImmutableSet.of(chosen), notChosen);
        }

        Iterable<Container> contentHierarchySourceOrderedContainers = Iterables.filter(
                orderedEquivalents,
                Container.class
        );
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

        Optional<Container> first = hierarchyChooser.chooseBestHierarchy(
                contentHierarchySourceOrderedContainers);

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
