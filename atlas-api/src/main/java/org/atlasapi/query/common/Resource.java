package org.atlasapi.query.common;

import com.google.common.collect.ImmutableSet;

public enum Resource {

    CONTENT("content", "content"),
    TOPIC("topic", "topics"),
    CHANNEL("channel", "channels"),
    CHANNEL_GROUP("channel_group", "channel_groups"),
    PERSON("person", "people"),
    APPLICATION("application", "applications"),
    SOURCE("source", "sources"),
    SOURCE_REQUEST("source_request", "source_requests"),
    USER("user", "users"),
    SOURCE_LICENSE("source_license", "source_licenses"),
    END_USER_LICENSE("end_user_license", "end_user_licenses"),
    MODEL_INFO("model_class", "model_classes"),
    ENDPOINT_INFO("endpoint", "endpoints"),
    EVENT("event", "events"),
    ORGANISATION("organisation", "organisations");

    private final String singular;
    private final String plural;

    private Resource(String singular, String plural) {
        this.singular = singular;
        this.plural = plural;
    }

    public String getSingular() {
        return this.singular;
    }

    public String getPlural() {
        return this.plural;
    }

    public String key() {
        return toString().toLowerCase();
    }

    private static final ImmutableSet<Resource> ALL = ImmutableSet.copyOf(values());

    public static ImmutableSet<Resource> all() {
        return ALL;
    }

}
