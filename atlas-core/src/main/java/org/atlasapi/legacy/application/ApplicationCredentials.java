package org.atlasapi.legacy.application;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.net.IpRange;

public class ApplicationCredentials {

    private final String apiKey;
    private final Set<IpRange> ipAddresses;

    public ApplicationCredentials(String apiKey, Set<IpRange> ipAddresses) {
        this.apiKey = checkNotNull(apiKey);
        this.ipAddresses = ipAddresses != null ? ImmutableSet.copyOf(ipAddresses) : ImmutableSet.<IpRange>of();
    }

    public ApplicationCredentials(String apiKey) {
        this(apiKey, ImmutableSet.<IpRange>of());
    }

    public String getApiKey() {
        return apiKey;
    }

    public ApplicationCredentials copyWithIpAddresses(Iterable<IpRange> ipAddresses) {
        return new ApplicationCredentials(apiKey, ImmutableSet.copyOf(ipAddresses));
    }

    public Set<IpRange> getIpAddressRanges() {
        return ipAddresses;
    }

}