package org.atlasapi.content;

import java.util.Currency;

import org.atlasapi.entity.Id;
import org.atlasapi.serialization.protobuf.ContentProtos;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class LocationSerializerTest {

    private final LocationSerializer serializer = new LocationSerializer();

    @Test
    public void testDeSerializeLocation() throws Exception {
        Location location = new Location();
        location.setEmbedCode("embedCode");
        location.setEmbedId("embedid");
        location.setTransportIsLive(true);
        location.setTransportSubType(TransportSubType.ITUNES);
        location.setTransportType(TransportType.LINK);
        location.setUri("uri");

        Policy policy = new Policy();
        policy.setActualAvailabilityStart(new DateTime(DateTimeZones.UTC));
        policy.setAvailabilityStart(new DateTime(DateTimeZones.UTC));
        policy.setAvailabilityLength(1234);
        policy.setAvailabilityEnd(new DateTime(DateTimeZones.UTC));
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        policy.setDrmPlayableFrom(new DateTime(DateTimeZones.UTC));
        policy.setNetwork(Policy.Network.WIFI);
        policy.setPlatform(Policy.Platform.IOS);
        policy.setServiceId(Id.valueOf(10L));
        policy.setPlayerId(Id.valueOf(20L));
        policy.setPrice(new Price(Currency.getInstance("GBP"), 400));
        policy.setRevenueContract(Policy.RevenueContract.PAY_TO_BUY);
        policy.setSubscriptionPackages(ImmutableSet.of("a", "b"));
        policy.setPricing(
                ImmutableList.of(
                        new Pricing(
                                DateTime.now(DateTimeZone.UTC),
                                DateTime.now(DateTimeZone.UTC).plusHours(1),
                                new Price(Currency.getInstance("GBP"), 1)
                        ),
                        new Pricing(
                                DateTime.now(DateTimeZone.UTC).minusHours(1),
                                DateTime.now(DateTimeZone.UTC).plusHours(1),
                                new Price(Currency.getInstance("USD"), 2)
                        )
                )
        );
        location.setPolicy(policy);

        byte[] bytes = serializer.serialize(location).build().toByteArray();

        Location deserialized = serializer.deserialize(ContentProtos.Location.parseFrom(bytes));

        assertThat(deserialized.getEmbedCode(), is(location.getEmbedCode()));
        assertThat(deserialized.getEmbedId(), is(location.getEmbedId()));
        assertThat(deserialized.getTransportIsLive(), is(location.getTransportIsLive()));
        assertThat(deserialized.getTransportSubType(), is(location.getTransportSubType()));
        assertThat(deserialized.getTransportType(), is(location.getTransportType()));
        assertThat(deserialized.getUri(), is(location.getUri()));

        Policy deserializedPolicy = deserialized.getPolicy();

        assertThat(
                deserializedPolicy.getActualAvailabilityStart(),
                is(policy.getActualAvailabilityStart())
        );
        assertThat(deserializedPolicy.getAvailabilityStart(), is(policy.getAvailabilityStart()));
        assertThat(deserializedPolicy.getAvailabilityLength(), is(policy.getAvailabilityLength()));
        assertThat(deserializedPolicy.getAvailabilityEnd(), is(policy.getAvailabilityEnd()));
        assertThat(deserializedPolicy.getAvailableCountries(), is(policy.getAvailableCountries()));
        assertThat(deserializedPolicy.getDrmPlayableFrom(), is(policy.getDrmPlayableFrom()));
        assertThat(deserializedPolicy.getNetwork(), is(policy.getNetwork()));
        assertThat(deserializedPolicy.getPlatform(), is(policy.getPlatform()));
        assertThat(deserializedPolicy.getServiceRef(), is(policy.getServiceRef()));
        assertThat(deserializedPolicy.getPlayerRef(), is(policy.getPlayerRef()));
        assertThat(deserializedPolicy.getPrice(), is(policy.getPrice()));
        assertThat(deserializedPolicy.getRevenueContract(), is(policy.getRevenueContract()));
        assertThat(
                deserializedPolicy.getSubscriptionPackages(),
                is(policy.getSubscriptionPackages())
        );
        assertThat(deserializedPolicy.getPricing(), is(policy.getPricing()));

    }

}
