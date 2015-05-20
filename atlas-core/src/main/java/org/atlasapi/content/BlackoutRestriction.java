package org.atlasapi.content;

import com.google.common.base.Objects;

import javax.annotation.Nullable;

public class BlackoutRestriction {

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