package org.atlasapi.equivalence;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;
import org.atlasapi.hashing.Hashable;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalenceRef implements Identifiable, Sourced, Hashable {

    public static final <T extends Identifiable & Sourced> Function<T, EquivalenceRef> toEquivalenceRef() {
        return new Function<T, EquivalenceRef>() {

            @Override
            public EquivalenceRef apply(T input) {
                return EquivalenceRef.valueOf(input);
            }
        };
    }

    public static final <T extends Identifiable & Sourced> EquivalenceRef valueOf(T t) {
        return new EquivalenceRef(t.getId(), t.getSource());
    }

    private final Id id;
    private final Publisher source;

    public EquivalenceRef(Id id, Publisher source) {
        this.source = checkNotNull(source);
        this.id = checkNotNull(id);
    }

    @FieldName("id")
    public Id getId() {
        return this.id;
    }

    @FieldName("source")
    public Publisher getSource() {
        return this.source;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EquivalenceRef) {
            EquivalenceRef other = (EquivalenceRef) that;
            return this.id.equals(other.id)
                    && this.source.equals(other.source);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id.hashCode() ^ source.hashCode();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add(source.toString(), id)
                .toString();
    }
}
