package org.atlasapi.application;

import org.atlasapi.model.translators.SourceStatusModelTranslator;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

public class SourceStatus {

    public static final SourceStatus UNAVAILABLE = new SourceStatus(SourceState.UNAVAILABLE, false);
    public static final SourceStatus REVOKED = new SourceStatus(SourceState.REVOKED, false);
    public static final SourceStatus REQUESTED = new SourceStatus(SourceState.REQUESTED, false);
    public static final SourceStatus AVAILABLE_ENABLED = new SourceStatus(SourceState.AVAILABLE, true);
    public static final SourceStatus AVAILABLE_DISABLED = new SourceStatus(SourceState.AVAILABLE, false);
    
    public static final Predicate<SourceStatus> IS_ENABLED = new Predicate<SourceStatus>(){

        @Override
        public boolean apply(SourceStatus input) {
            return input.isEnabled();
        }};
    
    public enum SourceState {
        
        UNAVAILABLE,
        REQUESTED,
        AVAILABLE,
        REVOKED
        
    }
    
    private final SourceState state;
    private final boolean enabled;

    public SourceStatus(SourceState state, boolean enabled) {
        Preconditions.checkArgument(!enabled || (state == SourceState.AVAILABLE && enabled));
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
    
    public SourceStatus approve() {
        Preconditions.checkState(state == SourceState.REQUESTED);
        return AVAILABLE_DISABLED;
    }
    
    public SourceStatus revoke() {
        Preconditions.checkState(state == SourceState.AVAILABLE);
        return REVOKED;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public SourceState getState() {
        return state;
    }
    
    public static SourceStatus fromV3SourceStatus(org.atlasapi.application.v3.SourceStatus sourceStatus) {
    	return SourceStatusModelTranslator.transform3To4(sourceStatus);
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
        return String.format("%s (%s)", state.toString().toLowerCase(), enabled ? "enabled" : "disabled");
    }
}
