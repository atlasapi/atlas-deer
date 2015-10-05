package org.atlasapi.content;

import org.atlasapi.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class Actor extends CrewMember {

    public Actor() {
        super();
        this.withRole(Role.ACTOR);
    }
    
    private String character;
    
    public Actor(String uri, String curie, Publisher publisher) {
         super(uri, curie, publisher);
         this.withRole(Role.ACTOR);
    }
    
    @FieldName("character")
    public String character() {
        return character;
    }
    
    public Actor withCharacter(String character) {
        this.character = character;
        return this;
    }
    
    @Override
    public Actor withName(String name) {
        this.name = name;
        return this;
    }
    
    @Override
    public Actor withProfileLink(String profileLink) {
        this.addAliasUrl(profileLink);
        return this;
    }
    
    public static Actor actorWithoutId(String name, String character, Publisher publisher) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Name is null or blank");
        Preconditions.checkNotNull(publisher);
        
        return new Actor(null, null, publisher)
            .withCharacter(character)
            .withName(name);
    }
    
    public static Actor actor(String id, String name, String character, Publisher publisher) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "ID is null or blank");
        Preconditions.checkNotNull(publisher);
        String uri = String.format(Person.BASE_URI, publisher.key(), id);
        String curie = publisher.key()+':'+id;
        return new Actor(uri, curie, publisher)
            .withCharacter(character)
            .withName(name);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Actor) {
            Actor actor = (Actor) obj;
            return super.equals(actor) && character.equals(character);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return this.getCanonicalUri().hashCode();
    }
    
    @Override
    public String toString() {
        return getCanonicalUri();//"Actor "+name+" plays "+character;
    }
    
    @Override
    public CrewMember copy() {
        Actor actor = new Actor();
        CrewMember.copyTo(actor, this);
        actor.character = character;
        return actor;
    }
}
