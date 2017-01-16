package org.atlasapi.application;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkArgument;

public class SourceStatus {

    public static final SourceStatus UNAVAILABLE = new SourceStatus(SourceState.UNAVAILABLE, false);
    public static final SourceStatus REVOKED = new SourceStatus(SourceState.REVOKED, false);
    public static final SourceStatus REQUESTED = new SourceStatus(SourceState.REQUESTED, false);
    public static final SourceStatus AVAILABLE_ENABLED = new SourceStatus(
            SourceState.AVAILABLE,
            true
    );
    public static final SourceStatus AVAILABLE_DISABLED = new SourceStatus(
            SourceState.AVAILABLE,
            false
    );

    private final SourceState state;
    private final boolean enabled;

    public SourceStatus(
            @Nullable SourceState state,
            boolean enabled
    ) {
        checkArgument(!enabled || (state == SourceState.AVAILABLE && enabled));
        this.state = state;
        this.enabled = enabled;
    }

    public SourceStatus copyWithState(SourceState state) {
        return new SourceStatus(state, isEnabled());
    }

    public SourceStatus enable() {
        Preconditions.checkState(state == SourceState.AVAILABLE);
        return AVAILABLE_ENABLED;
    }

    public SourceStatus disable() {
        Preconditions.checkState(state == SourceState.AVAILABLE);
        return AVAILABLE_DISABLED;
    }

    public SourceStatus request() {
        Preconditions.checkState(state == SourceState.UNAVAILABLE);
        return REQUESTED;
    }

    public SourceStatus deny() {
        Preconditions.checkState(state == SourceState.REQUESTED);
        return UNAVAILABLE;
    }

    public SourceStatus approve() {
        Preconditions.checkState(state == SourceState.REQUESTED);
        return AVAILABLE_DISABLED;
    }

    public SourceStatus revoke() {
        Preconditions.checkState(state == SourceState.AVAILABLE);
        return REVOKED;
    }

    public SourceStatus agreeLicense() {
        Preconditions.checkState(state == SourceState.ENABLEABLE);
        return AVAILABLE_DISABLED;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Nullable
    public SourceState getState() {
        return state;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof SourceStatus) {
            SourceStatus other = (SourceStatus) that;
            return enabled == other.enabled && state == other.state;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format(
                "%s (%s)",
                state.toString().toLowerCase(),
                enabled ? "enabled" : "disabled"
        );
    }

    public enum SourceState {
        UNAVAILABLE,
        REQUESTED,
        AVAILABLE,
        REVOKED,
        ENABLEABLE
    }
}
