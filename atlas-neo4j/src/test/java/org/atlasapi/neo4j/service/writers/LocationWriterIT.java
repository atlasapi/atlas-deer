package org.atlasapi.neo4j.service.writers;

import javax.annotation.Nullable;

import org.atlasapi.content.Encoding;
import org.atlasapi.content.Item;
import org.atlasapi.content.Location;
import org.atlasapi.content.Policy;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.AbstractNeo4jIT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class LocationWriterIT extends AbstractNeo4jIT {

    private ContentWriter contentWriter;
    private LocationWriter locationWriter;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        contentWriter = ContentWriter.create();
        locationWriter = LocationWriter.create();
    }

    @Test
    public void writeLocations() throws Exception {
        Location location = getLocation(true, getPolicy(
                DateTime.now(DateTimeZone.UTC).minusDays(1),
                DateTime.now(DateTimeZone.UTC).plusDays(1)
        ));
        Item item = getItem(
                0L,
                Publisher.METABROADCAST,
                ImmutableSet.of(location)
        );

        contentWriter.writeContent(item, session);
        locationWriter.write(item, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_LOCATION]->(l:Location)"
                        + "RETURN l.startDateTime AS startDateTime, l.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("startDateTime").asString(),
                is(location.getPolicy().getAvailabilityStart().toString()));
        assertThat(record.get("endDateTime").asString(),
                is(location.getPolicy().getAvailabilityEnd().toString()));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void writeLocationsWithNullStartEndDateTimes() throws Exception {
        Location location = getLocation(true, getPolicy(null, null));
        Item item = getItem(
                0L,
                Publisher.METABROADCAST,
                ImmutableSet.of(location)
        );

        contentWriter.writeContent(item, session);
        locationWriter.write(item, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_LOCATION]->(l:Location)"
                        + "RETURN l.startDateTime AS startDateTime, l.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("startDateTime").asString(),
                is(LocationWriter.AVAILABLE_FROM_FOREVER.toString()));
        assertThat(record.get("endDateTime").asString(),
                is(LocationWriter.AVAILABLE_UNTIL_FOREVER.toString()));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void doNotWriteLocationsWithNullPolicy() throws Exception {
        Location location = getLocation(false, getPolicy(
                DateTime.now(DateTimeZone.UTC).minusDays(1),
                DateTime.now(DateTimeZone.UTC).plusDays(1)
        ));
        Item item = getItem(
                0L,
                Publisher.METABROADCAST,
                ImmutableSet.of(location)
        );

        contentWriter.writeContent(item, session);
        locationWriter.write(item, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_LOCATION]->(l:Location)"
                        + "RETURN l.startDateTime AS startDateTime, l.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void doNotWriteLocationsWithAvailableFalse() throws Exception {
        Location location = getLocation(true, null);
        Item item = getItem(
                0L,
                Publisher.METABROADCAST,
                ImmutableSet.of(location)
        );

        contentWriter.writeContent(item, session);
        locationWriter.write(item, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_LOCATION]->(l:Location)"
                        + "RETURN l.startDateTime AS startDateTime, l.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void removeOldLocationsWhenWritingLocations() throws Exception {
        Location location = getLocation(true, getPolicy(
                DateTime.now(DateTimeZone.UTC).minusDays(1),
                DateTime.now(DateTimeZone.UTC).plusDays(1)
        ));
        Item item = getItem(
                0L,
                Publisher.METABROADCAST,
                ImmutableSet.of(location)
        );

        contentWriter.writeContent(item, session);
        locationWriter.write(item, session);

        Location updatedLocation = getLocation(true, getPolicy(
                DateTime.now(DateTimeZone.UTC).minusDays(0),
                DateTime.now(DateTimeZone.UTC).plusDays(2)
        ));
        Item updatedItem = getItem(
                0L,
                Publisher.METABROADCAST,
                ImmutableSet.of(updatedLocation)
        );

        locationWriter.write(updatedItem, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_LOCATION]->(l:Location)"
                        + "RETURN l.startDateTime AS startDateTime, l.endDateTime AS endDateTime",
                ImmutableMap.of("id", updatedItem.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("startDateTime").asString(),
                is(updatedLocation.getPolicy().getAvailabilityStart().toString()));
        assertThat(record.get("endDateTime").asString(),
                is(updatedLocation.getPolicy().getAvailabilityEnd().toString()));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void removeAllExistingLocations() throws Exception {
        Location location = getLocation(true, getPolicy(
                DateTime.now(DateTimeZone.UTC).minusDays(1),
                DateTime.now(DateTimeZone.UTC).plusDays(1)
        ));
        Item item = getItem(
                0L,
                Publisher.METABROADCAST,
                ImmutableSet.of(location)
        );

        contentWriter.writeContent(item, session);
        locationWriter.write(item, session);
        Item updatedItem = getItem(
                0L,
                Publisher.METABROADCAST,
                ImmutableSet.of()
        );

        locationWriter.write(updatedItem, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_LOCATION]->(l:Location)"
                        + "RETURN l.startDateTime AS startDateTime, l.endDateTime AS endDateTime",
                ImmutableMap.of("id", updatedItem.getId().longValue())
        );

        assertThat(result.hasNext(), is(false));
    }

    private Item getItem(long id, Publisher source, ImmutableSet<Location> locations) {
        Item item = new Item();

        item.setId(Id.valueOf(id));
        item.setPublisher(source);

        Encoding encoding = new Encoding();
        encoding.setAvailableAt(locations);

        item.setManifestedAs(ImmutableSet.of(encoding));

        return item;
    }

    private Location getLocation(boolean available, @Nullable Policy policy) {
        Location location = new Location();
        location.setAvailable(available);

        location.setPolicy(policy);

        return location;
    }

    private Policy getPolicy(@Nullable DateTime start, @Nullable DateTime end) {
        Policy policy = new Policy();

        policy.setAvailabilityStart(start);
        policy.setAvailabilityEnd(end);

        return policy;
    }
}
