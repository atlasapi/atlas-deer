package org.atlasapi.content.v2.model;

/** This combines the two volatile update time fields with the rest of the fields
 * from {@link org.atlasapi.entity.Identified} to make the full interface. Most things want to use
 * this, unless they need the update times split off for reasons of key stability.
 */
public interface Identified extends WithUpdateTimes, IdentifiedWithoutUpdateTimes {

}
