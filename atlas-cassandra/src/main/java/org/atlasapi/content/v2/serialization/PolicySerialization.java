package org.atlasapi.content.v2.serialization;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.v2.model.udt.Policy;
import org.atlasapi.content.v2.model.udt.Pricing;
import org.atlasapi.entity.Id;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

import org.joda.time.DateTime;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;

public class PolicySerialization {

    private final IdentifiedSetter identifiedSetter = new IdentifiedSetter();
    private final PriceSerialization price = new PriceSerialization();
    private final PricingSerialization pricing = new PricingSerialization();


    public Policy serialize(org.atlasapi.content.Policy policy) {
        if (policy == null) {
            return null;
        }
        Policy internal = new Policy();

        identifiedSetter.serialize(internal, policy);

        DateTime availabilityStart = policy.getAvailabilityStart();
        if (availabilityStart != null) {
            internal.setAvailabilityStart(availabilityStart.toInstant());
        }
        DateTime availabilityEnd = policy.getAvailabilityEnd();
        if (availabilityEnd != null) {
            internal.setAvailabilityEnd(availabilityEnd.toInstant());
        }
        DateTime drmPlayableFrom = policy.getDrmPlayableFrom();
        if (drmPlayableFrom != null) {
            internal.setDrmPlayableFrom(drmPlayableFrom.toInstant());
        }
        internal.setAvailableCountries(policy.getAvailableCountries()
                .stream()
                .map(Country::code)
                .collect(Collectors.toSet()));
        internal.setAvailabilityLength(policy.getAvailabilityLength());
        org.atlasapi.content.Policy.RevenueContract revenueContract = policy.getRevenueContract();
        if (revenueContract != null) {
            internal.setRevenueContract(revenueContract.key());
        }
        internal.setSubscriptionPackages(policy.getSubscriptionPackages());
        internal.setPrice(price.serialize(policy.getPrice()));
        internal.setPricing(policy.getPricing()
                .stream()
                .map(pricing::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        Id serviceRef = policy.getServiceRef();
        if (serviceRef != null) {
            internal.setServiceId(serviceRef.longValue());
        }
        Id playerRef = policy.getPlayerRef();
        if (playerRef != null) {
            internal.setPlayerId(playerRef.longValue());
        }
        org.atlasapi.content.Policy.Platform platform = policy.getPlatform();
        if (platform != null) {
            internal.setPlatform(platform.key());
        }
        org.atlasapi.content.Policy.Network network = policy.getNetwork();
        if (network != null) {
            internal.setNetwork(network.key());
        }
        DateTime actualAvailabilityStart = policy.getActualAvailabilityStart();
        if (actualAvailabilityStart != null) {
            internal.setActualAvailabilityStart(actualAvailabilityStart.toInstant());
        }

        return internal;
    }

    public org.atlasapi.content.Policy deserialize(Policy internal) {
        if (internal == null) {
            return null;
        }
        org.atlasapi.content.Policy policy = new org.atlasapi.content.Policy();

        identifiedSetter.deserialize(policy, internal);

        policy.setAvailabilityStart(toDateTime(internal.getAvailabilityStart()));
        policy.setAvailabilityEnd(toDateTime(internal.getAvailabilityEnd()));
        policy.setDrmPlayableFrom(toDateTime(internal.getDrmPlayableFrom()));

        Set<String> availableCountries = internal.getAvailableCountries();
        if (availableCountries != null) {
            policy.setAvailableCountries(availableCountries.stream()
                    .map(Countries::fromCode)
                    .collect(Collectors.toSet()));
        }

        policy.setAvailabilityLength(internal.getAvailabilityLength());
        String revenueContract = internal.getRevenueContract();
        if (revenueContract != null) {
            policy.setRevenueContract(org.atlasapi.content.Policy.RevenueContract.fromKey(
                    revenueContract));
        }
        policy.setSubscriptionPackages(internal.getSubscriptionPackages());
        org.atlasapi.content.v2.model.udt.Price internalPrice = internal.getPrice();
        if (internalPrice != null) {
            policy.setPrice(price.deserialize(internalPrice));
        }
        List<Pricing> pricing = internal.getPricing();
        if (pricing != null) {
            policy.setPricing(pricing.stream()
                    .map(this.pricing::deserialize)
                    .collect(Collectors.toList()));
        }

        Long serviceId = internal.getServiceId();
        if (serviceId != null) {
            policy.setServiceId(Id.valueOf(serviceId));
        }
        Long playerId = internal.getPlayerId();
        if (playerId != null) {
            policy.setPlayerId(Id.valueOf(playerId));
        }
        if (internal.getPlatform() != null) {
            policy.setPlatform(org.atlasapi.content.Policy.Platform.fromKey(internal.getPlatform()));
        }
        if (internal.getNetwork() != null) {
            policy.setNetwork(org.atlasapi.content.Policy.Network.fromKey(internal.getNetwork()));
        }
        policy.setActualAvailabilityStart(toDateTime(internal.getActualAvailabilityStart()));

        return policy;
    }

}