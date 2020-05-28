package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;

public class Clip extends Item {

    private String clipOf;

    public Clip(String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
    }

    public Clip(Id id, Publisher source) {
        super(id, source);
    }

    public void setClipOf(String clipOf) {
        this.clipOf = clipOf;
    }

    @FieldName("clip_of")
    public String getClipOf() {
        return clipOf;
    }

    @Override
    public ClipRef toRef() {
        return new ClipRef(
                getId(),
                getSource(),
                SortKey.keyFrom(this),
                getThisOrChildLastUpdated()
        );
    }

    public Clip() {
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (getCanonicalUri() != null && obj instanceof Clip) {
            return getCanonicalUri().equals(((Clip) obj).getCanonicalUri());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getCanonicalUri() == null ? super.hashCode()
                                         : getCanonicalUri().hashCode();
    }

    public static Clip copyTo(Clip from, Clip to) {
        Item.copyTo(from, to);
        to.clipOf = from.clipOf;
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Clip) {
            copyTo(this, (Clip) to);
            return to;
        }
        return super.copyTo(to);
    }

    @Override public Clip copy() {
        return copyTo(this, new Clip());
    }

    @Override
    public Clip createNew() {
        return new Clip();
    }

    @Override
    public <V> V accept(ItemVisitor<V> visitor) {
        return visitor.visit(this);
    }

}
