package org.atlasapi.content;

import javax.annotation.Nullable;

import org.atlasapi.hashing.Hashable;

import com.google.common.base.Objects;

public class BlackoutRestriction implements Hashable {

    private Boolean all;

    public BlackoutRestriction(@Nullable Boolean all) {
        this.all = all;
    }

    public Boolean getAll() {
        return all;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (!(that instanceof BlackoutRestriction)) {
            return false;
        }

        BlackoutRestriction thatBlackoutRestriction = (BlackoutRestriction) that;
        return Objects.equal(this.all, thatBlackoutRestriction.all);
    }
}
