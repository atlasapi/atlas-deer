/* Copyright 2010 Meta Broadcast Ltd

 Licensed under the Apache License, Version 2.0 (the "License"); you
 may not use this file except in compliance with the License. You may
 obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License. */
package org.atlasapi.content;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.entity.Aliased;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Sourced;
import org.atlasapi.equivalence.Equivalable;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.event.EventRef;
import org.atlasapi.hashing.ExcludeFromHash;
import org.atlasapi.hashing.Hashable;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.metabroadcast.common.intl.Country;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Content extends Described
        implements Aliased, Sourced, Equivalable<Content>, Hashable {

    @ExcludeFromHash
    private transient String readHash;

    private ImmutableList<Clip> clips = ImmutableList.of();
    private Set<KeyPhrase> keyPhrases = ImmutableSet.of();
    private ImmutableList<Tag> tags = ImmutableList.of();
    private ImmutableList<ContentGroupRef> contentGroupRefs = ImmutableList.of();
    private List<CrewMember> people = Lists.newArrayList();
    private Set<String> languages = ImmutableSet.of();
    private Set<Certificate> certificates = ImmutableSet.of();
    private Integer year = null;
    @Nullable private Set<Encoding> manifestedAs = Sets.newLinkedHashSet();
    private Boolean genericDescription = Boolean.FALSE;
    private ImmutableSet<EventRef> eventRefs = ImmutableSet.of();
    private Set<Country> countriesOfOrigin = ImmutableSet.of();

    public Content(String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
    }

    public Content() {
        // some legacy code still requires a default constructor
    }

    public Content(Id id, Publisher source) {
        super(id, source);
    }

    @FieldName("clips")
    public List<Clip> getClips() {
        return clips;
    }

    public void setTags(Iterable<Tag> tags) {
        this.tags = ImmutableList.copyOf(tags);
    }

    public void addTopicRef(Tag tag) {
        tags = ImmutableList.<Tag>builder().add(tag).addAll(tags).build();
    }

    @FieldName("topic_refs")
    public List<Tag> getTags() {
        return tags;
    }

    public void setContentGroupRefs(Iterable<ContentGroupRef> contentGroupRefs) {
        this.contentGroupRefs = ImmutableList.copyOf(contentGroupRefs);
    }

    public void addContentGroup(ContentGroupRef contentGroupRef) {
        contentGroupRefs = ImmutableList.<ContentGroupRef>builder().add(contentGroupRef)
                .addAll(contentGroupRefs)
                .build();
    }

    @FieldName("content_group_refs")
    public List<ContentGroupRef> getContentGroupRefs() {
        return contentGroupRefs;
    }

    public void setClips(Iterable<Clip> clips) {
        this.clips = ImmutableList.copyOf(clips);
        for (Clip clip : clips) {
            clip.setClipOf(this.getCanonicalUri());
        }
    }

    public void addClip(Clip clip) {
        List<Clip> all = Lists.newArrayList(clips);
        all.add(clip);
        setClips(all);
    }

    @FieldName("key_phrases")
    public Set<KeyPhrase> getKeyPhrases() {
        return keyPhrases;
    }

    public void setKeyPhrases(Iterable<KeyPhrase> phrases) {
        keyPhrases = ImmutableSet.copyOf(phrases);
    }

    public void addKeyPhrase(KeyPhrase phrase) {
        keyPhrases = ImmutableSet.<KeyPhrase>builder().add(phrase).addAll(keyPhrases).build();
    }

    @FieldName("people")
    public List<CrewMember> people() {
        return people;
    }

    @FieldName("actors")
    public List<Actor> actors() {
        return Lists.newArrayList(Iterables.filter(people, Actor.class));
    }

    public void addPerson(CrewMember person) {
        people.add(person);
    }

    public void setPeople(List<CrewMember> people) {
        this.people = people == null ? Lists.newArrayList() : people;
    }

    @FieldName("genericDescription")
    public Content setGenericDescription(Boolean isGenericDescription) {
        this.genericDescription = isGenericDescription;
        return this;
    }

    @FieldName("event_refs")
    public Set<EventRef> getEventRefs() {
        return eventRefs;
    }

    public void setEventRefs(Iterable<EventRef> eventsRef) {
        this.eventRefs = ImmutableSet.copyOf(eventsRef);
    }

    public void addEventRef(EventRef eventRef) {
        eventRefs = ImmutableSet.<EventRef>builder().add(eventRef).addAll(eventRefs).build();
    }

    @FieldName("countries_of_origin")
    public Set<Country> getCountriesOfOrigin() {
        return countriesOfOrigin;
    }

    public void setCountriesOfOrigin(Iterable<Country> countries) {
        this.countriesOfOrigin = ImmutableSet.copyOf(countries);
    }

    public void addCountryOfOrigin(Country country) {
        countriesOfOrigin = ImmutableSet.<Country>builder().add(country).addAll(countriesOfOrigin).build();
    }

    public Boolean isGenericDescription() {
        return genericDescription;
    }

    public static Content copyTo(Content from, Content to) {
        Described.copyTo(from, to);
        to.clips = ImmutableList.copyOf(Iterables.transform(from.clips, Clip::copy));
        to.keyPhrases = from.keyPhrases;
        to.relatedLinks = from.relatedLinks;
        to.tags = from.tags;
        to.readHash = from.readHash;
        to.people = Lists.newArrayList(Iterables.transform(from.people, CrewMember::copy));
        to.languages = from.languages;
        to.certificates = from.certificates;
        to.year = from.year;
        to.manifestedAs = from.manifestedAs == null ? null : Sets.newLinkedHashSet(from.manifestedAs);
        to.genericDescription = from.genericDescription;
        to.eventRefs = from.eventRefs;
        to.countriesOfOrigin = ImmutableSet.copyOf(from.countriesOfOrigin);
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Content) {
            copyTo(this, (Content) to);
            return to;
        }
        return super.copyTo(to);
    }

    public void setReadHash(@Nullable String readHash) {
        this.readHash = readHash;
    }

    public boolean hashChanged(String newHash) {
        return readHash == null || !this.readHash.equals(newHash);
    }

    protected String getSortKey() {
        return SortKey.DEFAULT.name();
    }

    @FieldName("languages")
    public Set<String> getLanguages() {
        return languages;
    }

    public void setLanguages(Iterable<String> languages) {
        this.languages = ImmutableSet.copyOf(languages);
    }

    @FieldName("certificates")
    public Set<Certificate> getCertificates() {
        return certificates;
    }

    public void setCertificates(Iterable<Certificate> certificates) {
        this.certificates = ImmutableSet.copyOf(certificates);
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    @FieldName("year")
    public Integer getYear() {
        return year;
    }

    public abstract <V> V accept(ContentVisitor<V> visitor);

    public abstract ContentRef toRef();

    public static final Function<Content, ContentRef> toContentRef() {
        return ToContentRefFunction.INSTANCE;
    }

    @FieldName("manifested_as")
    @Nullable
    public Set<Encoding> getManifestedAs() {
        return manifestedAs;
    }

    public void setManifestedAs(@Nullable Set<Encoding> manifestedAs) {
        this.manifestedAs = manifestedAs;
    }

    public void addManifestedAs(Encoding encoding) {
        checkNotNull(encoding);
        manifestedAs.add(encoding);
    }

    private enum ToContentRefFunction implements Function<Content, ContentRef> {
        INSTANCE;

        @Override
        public ContentRef apply(Content input) {
            return input.toRef();
        }

    }

    @Override
    public Content copyWithEquivalentTo(Iterable<EquivalenceRef> refs) {
        super.copyWithEquivalentTo(refs);
        return this;
    }

    public Iterable<LocationSummary> getAvailableLocations() {
        return manifestedAs
                .stream()
                .flatMap(e -> e.getAvailableAt().stream())
                .filter(Location::isAvailable)
                .map(Location::toSummary)
                .collect(Collectors.toSet());
    }
}
