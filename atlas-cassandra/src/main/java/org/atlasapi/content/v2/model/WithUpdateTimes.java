package org.atlasapi.content.v2.model;

import org.joda.time.Instant;

/** Contains some volatile update time fields. Split off because these make
 * using an {@link org.atlasapi.entity.Identified} for a CQL map key impossible.
 *
 * @see Identified
 * @see IdentifiedWithoutUpdateTimes
 */
public interface WithUpdateTimes {

    Instant getLastUpdated();

    void setLastUpdated(Instant lastUpdated);

    Instant getEquivalenceUpdate();

    void setEquivalenceUpdate(Instant equivalenceUpdate);
}
