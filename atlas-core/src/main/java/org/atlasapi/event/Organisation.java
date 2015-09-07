package org.atlasapi.event;

import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Person;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

public class Organisation extends ContentGroup {

    private ImmutableList<Person> members;
    
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
