package org.atlasapi.organisation;

import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.Described;
import org.atlasapi.entity.Person;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class Organisation extends ContentGroup {

    private ImmutableList<Person> members;

    private ImmutableSet<String> alternativeTitles;

    public Organisation() {
        this(ImmutableList.of());
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

    public static Organisation copyTo(Organisation from, Organisation to) {
        ContentGroup.copyTo(from, to);
        to.members = from.members;
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Organisation) {
            copyTo(this, (Organisation) to);
            return to;
        }
        return super.copyTo(to);
    }

    @Override public Organisation copy() {
        return copyTo(this, new Organisation());
    }
}
