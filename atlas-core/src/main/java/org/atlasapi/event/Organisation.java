package org.atlasapi.event;

import org.atlasapi.content.ContentGroup;
import org.atlasapi.entity.Person;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class Organisation extends ContentGroup {

    private ImmutableList<Person> members;

    private ImmutableSet<String> alternativeTitles;
    
    public Organisation() {
        this(ImmutableList.<Person>of());
    }
    
    public Organisation(Iterable<Person> members) {
        this.members = ImmutableList.copyOf(members);
    }
    
    public ImmutableList<Person> members() {
        return members;
    }
    
    public void setMembers(Iterable<Person> members) {
        this.members = ImmutableList.copyOf(members);
    }

    public void setAlternativeTitles(Iterable<String> alternativeTitles) {
        this.alternativeTitles = ImmutableSet.copyOf(alternativeTitles);
    }

    public ImmutableSet<String> getAlternativeTitles() {
        return alternativeTitles;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(Organisation.class)
                .add("title", getTitle())
                .add("members", members)
                .add("uri", getCanonicalUri())
                .toString();
    }
    
    @Override
    public Organisation copy() {
        Organisation copy = new Organisation(this.members);
        ContentGroup.copyTo(this, copy);
        return copy;
    }
}
