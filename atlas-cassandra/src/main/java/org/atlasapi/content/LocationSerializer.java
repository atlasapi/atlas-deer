package org.atlasapi.content;

import java.util.Currency;

import org.atlasapi.content.Policy.RevenueContract;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.entity.Id;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Location.Builder;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableSet;

public class LocationSerializer {

    private final PricingSerializer pricingSerializer = new PricingSerializer();
    private final DateTimeSerializer dateTimeSerializer = new DateTimeSerializer();

    public ContentProtos.Location.Builder serialize(Location location) {
        Builder builder = ContentProtos.Location.newBuilder();
        if (location.getEmbedCode() != null) {
            builder.setEmbedCode(location.getEmbedCode());
        }
        if (location.getEmbedId() != null) {
            builder.setEmbedId(location.getEmbedId());
        }
        if (location.getTransportIsLive() != null) {
            builder.setTransportIsLive(location.getTransportIsLive());
        }
        if (location.getTransportSubType() != null) {
            builder.setTransportSubType(location.getTransportSubType().toString());
        }
        if (location.getTransportType() != null) {
            builder.setTransportType(location.getTransportType().toString());
        }
        if (location.getUri() != null) {
            builder.setUri(location.getUri());
        }
        for (Alias alias : location.getAliases()) {
            builder.addAliases(CommonProtos.Alias.newBuilder()
                    .setNamespace(alias.getNamespace())
                    .setValue(alias.getValue()));
        }

        Policy policy = location.getPolicy();
        if (policy == null) {
            return builder;
        }

        if (policy.getActualAvailabilityStart() != null) {
            builder.setActualAvailabilityStart(dateTimeSerializer.serialize(policy.getActualAvailabilityStart()));
        }
        if (policy.getAvailabilityStart() != null) {
            builder.setAvailabilityStart(dateTimeSerializer.serialize(policy.getAvailabilityStart()));
        }
        if (policy.getAvailabilityEnd() != null) {
            builder.setAvailabilityEnd(dateTimeSerializer.serialize(policy.getAvailabilityEnd()));
        }
        if (policy.getAvailabilityLength() != null) {
            builder.setAvailabilityLength(policy.getAvailabilityLength());
        }
        builder.addAllAvailableCountries(Countries.toCodes(policy.getAvailableCountries()));

        if (policy.getDrmPlayableFrom() != null) {
            builder.setDrmPlayableFrom(dateTimeSerializer.serialize(policy.getDrmPlayableFrom()));
        }
        if (policy.getNetwork() != null) {
            builder.setNetwork(policy.getNetwork().key());
        }
        if (policy.getPlatform() != null) {
            builder.setPlatform(policy.getPlatform().key());
        }
        if (policy.getServiceRef() != null) {
            builder.setServiceId(policy.getServiceRef().longValue());
        }
        if (policy.getPlayerRef() != null) {
            builder.setPlayerId(policy.getPlayerRef().longValue());
        }
        if (policy.getPrice() != null) {
            builder.setCurrency(policy.getPrice().getCurrency().getCurrencyCode());
            builder.setAmount(policy.getPrice().getAmount());
        }
        if (policy.getRevenueContract() != null) {
            builder.setRevenueContract(policy.getRevenueContract().key());
        }
        if (policy.getSubscriptionPackages() != null) {
            builder.addAllSubscriptionPackages(policy.getSubscriptionPackages());
        }
        if (!policy.getPricing().isEmpty()) {
            builder.addAllPricing(
                    policy.getPricing().stream()
                            .map(pricingSerializer::serialize)
                            .collect(MoreCollectors.toImmutableList())
            );
        }

        return builder;
    }

    public Location deserialize(ContentProtos.Location msg) {
        Location location = new Location();
        location.setEmbedCode(msg.hasEmbedCode() ? msg.getEmbedCode() : null);
        location.setEmbedId(msg.hasEmbedId() ? msg.getEmbedId() : null);
        location.setTransportIsLive(msg.hasTransportIsLive() ? msg.getTransportIsLive() : null);
        location.setUri(msg.hasUri() ? msg.getUri() : null);
        if (msg.hasTransportType()) {
            location.setTransportType(TransportType.fromString(msg.getTransportType()));
        }
        if (msg.hasTransportSubType()) {
            location.setTransportSubType(TransportSubType.fromString(msg.getTransportSubType()));
        }

        Policy policy = new Policy();

        if (msg.hasActualAvailabilityStart()) {
            policy.setActualAvailabilityStart(dateTimeSerializer.deserialize(msg.getActualAvailabilityStart()));
        }
        if (msg.hasAvailabilityStart()) {
            policy.setAvailabilityStart(dateTimeSerializer.deserialize(msg.getAvailabilityStart()));
        }
        if (msg.hasAvailabilityEnd()) {
            policy.setAvailabilityEnd(dateTimeSerializer.deserialize(msg.getAvailabilityEnd()));
        }
        policy.setAvailabilityLength(msg.hasAvailabilityLength()
                                     ? msg.getAvailabilityLength()
                                     : null);
        policy.setAvailableCountries(Countries.fromCodes(msg.getAvailableCountriesList()));

        if (msg.hasDrmPlayableFrom()) {
            policy.setDrmPlayableFrom(dateTimeSerializer.deserialize(msg.getDrmPlayableFrom()));
        }
        if (msg.hasNetwork()) {
            policy.setNetwork(Policy.Network.fromKey(msg.getNetwork()));
        }
        if (msg.hasPlatform()) {
            policy.setPlatform(Policy.Platform.fromKey(msg.getPlatform()));
        }
        if (msg.hasPlayerId()) {
            policy.setPlayerId(Id.valueOf(msg.getPlayerId()));
        }
        if (msg.hasServiceId()) {
            policy.setServiceId(Id.valueOf(msg.getServiceId()));
        }
        if (msg.hasAmount() && msg.hasCurrency()) {
            policy.setPrice(new Price(
                    Currency.getInstance(msg.getCurrency()),
                    msg.getAmount()
            ));
        }
        if (msg.hasRevenueContract()) {
            policy.setRevenueContract(RevenueContract.fromKey(msg.getRevenueContract()));
        }
        policy.setSubscriptionPackages(msg.getSubscriptionPackagesList());
        if (msg.getPricingCount() > 0) {
            policy.setPricing(
                    msg.getPricingList()
                            .stream()
                            .map(pricingSerializer::deserialize)
                            .collect(MoreCollectors.toImmutableList())
            );
        }
        location.setPolicy(policy);

        ImmutableSet.Builder<Alias> aliases = ImmutableSet.builder();
        for (CommonProtos.Alias alias : msg.getAliasesList()) {
            aliases.add(new Alias(alias.getNamespace(), alias.getValue()));
        }
        location.setAliases(aliases.build());
        return location;
    }

}
