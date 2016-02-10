package org.atlasapi.model.translators;

import org.atlasapi.application.SourceStatus;

public class SourceStatusModelTranslator {

    public static SourceStatus transform3To4(
            org.atlasapi.application.v3.SourceStatus sourceStatus) {
        return new SourceStatus(transform3To4(sourceStatus.getState()), sourceStatus.isEnabled());
    }

    public static SourceStatus.SourceState transform3To4(
            org.atlasapi.application.v3.SourceStatus.SourceState sourceState) {
        return SourceStatus.SourceState.valueOf(sourceState.name().toString());
    }

    public static org.atlasapi.application.v3.SourceStatus transform4To3(
            SourceStatus sourceStatus) {
        org.atlasapi.application.v3.SourceStatus v3Status = org.atlasapi.application.v3.SourceStatus
                .UNAVAILABLE.copyWithState(transform4To3(sourceStatus.getState()));
        if (sourceStatus.getState().equals(SourceStatus.SourceState.AVAILABLE)) {
            if (sourceStatus.isEnabled()) {
                return v3Status.enable();
            } else {
                return v3Status.disable();
            }
        } else {
            return v3Status;
        }
    }

    public static org.atlasapi.application.v3.SourceStatus.SourceState transform4To3(
            SourceStatus.SourceState sourceState) {
        return org.atlasapi.application.v3.SourceStatus.SourceState.valueOf(sourceState.name());
    }
}
