package org.atlasapi.content;

import com.google.common.base.Objects;
import org.atlasapi.equivalence.Equivalable;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class ContentGroup extends Described implements MutableContentList, Equivalable<ContentGroup> {

    private transient String readHash;
    //
    private Type type;
    private ImmutableSet<ContentRef> contents = ImmutableSet.of();

    public ContentGroup(String uri) {
        super(uri);
        this.type = Type.PLAYLIST;
    }

    public ContentGroup(String uri, Publisher publisher) {
        super(uri, null, publisher);
        this.type = Type.PLAYLIST;
    }
    
    protected ContentGroup(Type type, String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
        this.type = type;
    }

    public ContentGroup() {
        this.type = Type.PLAYLIST;
    }

    @FieldName("contents")
    public ImmutableList<ContentRef> getContents() {
        return contents.asList();
    }

    public void setType(Type type) {
        this.type = type;
    }

    @FieldName("type")
    public Type getType() {
        return type;
    }

    public void setContents(Iterable<ContentRef> children) {
        this.contents = ImmutableSet.copyOf(children);
    }

    public void addContent(ContentRef childRef) {
        this.contents = ImmutableSet.<ContentRef>builder().addAll(this.getContents()).add(childRef).build();
    }

    public void addContents(Iterable<ContentRef> childRef) {
        this.contents = ImmutableSet.<ContentRef>builder().addAll(this.getContents()).addAll(childRef).build();
    }
    
    public void setReadHash(String readHash) {
        this.readHash = readHash;
    }
    
    public boolean hashChanged(String newHash) {
        return readHash == null || !this.readHash.equals(newHash);
    }

    public ContentGroup copy() {
        ContentGroup copy = new ContentGroup();
        copyTo(this, copy);
        return copy;
    }
    
    public ContentGroupRef contentGroupRef() {
        return new ContentGroupRef(getId(), getCanonicalUri());
    }

    public enum Type {
        FRANCHISE, SEASON, PLAYLIST, PERSON, ORGANISATION;
    }
    
    @Override
    public ContentGroup copyWithEquivalentTo(Iterable<EquivalenceRef> refs) {
        super.copyWithEquivalentTo(refs);
        return this;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", getId())
                .add("contents", contents)
                .toString();
    }
}
