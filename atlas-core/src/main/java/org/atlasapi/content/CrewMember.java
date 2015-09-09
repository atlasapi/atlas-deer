package org.atlasapi.content;

import java.util.Map;
import java.util.Set;

import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.base.Maybe;

public class CrewMember extends Identified {
    
    public enum Role {
        ABRIDGED_BY("abridged_by", "Abridged By"),
        ART_DIRECTOR("art_director", "Art Director"), 
        SUPERVISING_DIRECTOR("supervising_director", "Supervising Director"),
        SOURCE_WRITER("source_writer", "Source Writer"),
        ASSISTANT_DIRECTOR("assistant_director", "Assistant Director"),
        ASSOCIATE_DIRECTOR("associate_director", "Associate Director"),
        ADDITIONAL_DIRECTOR("additional_director", "Additional Director"),
        COLLABORATING_DIRECTOR("collaborating_director", "Collaborating Director"),
        CO_DIRECTOR("co-director", "Co-Director"),
        DEPUTY_EDITOR("deputy_editor", "Deputy Editor"),
        DIRECTOR("director", "Director"),
        DRAMATISED_BY("dramatised_by", "Dramatised By"),
        EDITOR("editor", "Editor"),
        EXECUTIVE_EDITOR("executive_editor", "Executive Editor"),
        EXECUTIVE_PRODUCER("executive_producer", "Executive Producer"),
        PRODUCER("producer", "Producer"),
        SERIES_DIRECTOR("series_director", "Series Director"),
        SERIES_EDITOR("series_editor", "Series Editor"),
        SERIES_PRODUCER("series_producer", "Series Producer"),
        WRITER("writer", "Writer"),
        ADAPTED_BY("adapted_by", "Adapted By"),
        PRESENTER("presenter", "Presenter"),
        COMPOSER("composer", "Composer"),
        ACTOR("actor", "Actor"),
        PARTICIPANT("participant", "Participant"),
        NARRATOR("narrator", "Narrator"),
        REPORTER("reporter", "Reporter"),
        COMMENTATOR("commentator", "Commentator"),
        EXPERT("expert", "Expert"),
        ARTIST("artist", "Artist"),
        CONTRIBUTOR("contributor", "Contributor");
        
        private final String key;
        private final String title;
        
        Role(String key, String title) {
            this.key = key;
            this.title = title;
        }
        
        @FieldName("key")
        public String key() {
            return key;
        }
        
        @FieldName("title")
        public String title() {
            return title;
        }
        
        public static Role fromKey(String key) {
            Maybe<Role> role = fromPossibleKey(key);
            if (role.isNothing()) {
            	throw new IllegalArgumentException("Unknown role: "+key);
            }
            return role.requireValue();
        }

        public static Maybe<Role> fromPossibleKey(String key) {
            Maybe<Role> possibleRole = roleKeyMap.get(key);
            return possibleRole != null ? possibleRole : Maybe.<Role>nothing();
        }
        
		private static Map<String,Maybe<Role>> roleKeyMap = initRoleKeyMap();

        private static Map<String, Maybe<Role>> initRoleKeyMap() {
            ImmutableMap.Builder<String, Maybe<Role>> builder = ImmutableMap.builder();
            for (Role role : Role.values()) {
                builder.put(role.key(), Maybe.just(role));
            }
            return builder.build();
        }
    }
    
    private Role role;
    protected String name;
    private Publisher publisher;
    
    public CrewMember() {
        super();
    }
    
    public CrewMember(String uri, String curie, Publisher publisher) {
        super(uri, curie);
        this.publisher = publisher;
    }
    
    @FieldName("role")
    public Role role() {
        return role;
    }
    
    @FieldName("name")
    public String name() {
        return name;
    }
    
    @FieldName("publisher")
    public Publisher publisher() {
        return publisher;
    }
    
    @FieldName("profile_links")
    public Set<String> profileLinks() {
        return getAliasUrls();
    }
    
    public CrewMember withPublisher(Publisher publisher) {
        this.publisher = publisher;
        return this;
    }
    
    public CrewMember withRole(Role role) {
        this.role = role;
        return this;
    }
    
    public CrewMember withName(String name) {
        this.name = name;
        return this;
    }
    
    public CrewMember withProfileLink(String profileLink) {
        this.addAliasUrl(profileLink);
        return this;
    }
    
    public CrewMember withProfileLinks(Set<String> profileLinks) {
        this.setAliasUrls(profileLinks);
        return this;
    }
    
    public static CrewMember crewMemberWithoutId(String name, String roleKey, Publisher publisher) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Name is null or blank");
        Preconditions.checkNotNull(publisher);
        
        Role role = Role.fromKey(roleKey);
        return new CrewMember(null, null, publisher)
            .withRole(role)
            .withName(name);
    }
    
    public static CrewMember crewMember(String id, String name, String roleKey, Publisher publisher) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "ID is null or blank");
        Preconditions.checkNotNull(publisher);
        Role role = Role.fromKey(roleKey);
        String uri = String.format(Person.BASE_URI, publisher.key(), id);
        String curie = publisher.key()+':'+id;
        return new CrewMember(uri, curie, publisher)
            .withRole(role)
            .withName(name);
    }
    
    public Person toPerson() {
        return new Person(this.getCanonicalUri(), this.getCurie(), this.publisher()).withName(name).withProfileLinks(profileLinks());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CrewMember) {
            CrewMember crew = (CrewMember) obj;
            return this.getCanonicalUri().equals(crew.getCanonicalUri()) && name.equals(crew.name) && role == crew.role;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return getCanonicalUri().hashCode();
    }
    
    @Override
    public String toString() {
        return getCanonicalUri();//"Crew "+name+" worked as a "+role.title;
    }
    
    public CrewMember copy() {
        CrewMember crew = new CrewMember();
        copyTo(crew, this);
        return crew;
    }
    
    public static void copyTo(CrewMember to, CrewMember from) {
        Identified.copyTo(from, to);
        to.name = from.name;
        to.publisher = from.publisher;
        to.role = from.role;
    }
    
    public final static Function<CrewMember, CrewMember> COPY = new Function<CrewMember, CrewMember>() {
        @Override
        public CrewMember apply(CrewMember input) {
            return input.copy();
        }
    };
}
