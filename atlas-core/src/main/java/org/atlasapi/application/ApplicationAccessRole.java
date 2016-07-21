package org.atlasapi.application;

import java.util.Arrays;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Defines roles for application Role Based Access Control.
 */
public enum ApplicationAccessRole {
    /**
     * Allows access to the the Owl APIs
     */
    OWL_ACCESS("owl-access"),

    /**
     * Allows access to the Deer APIs
     */
    DEER_ACCESS("deer-access"),

    /**
     * Allows access to API features that have been sunsetted and are scheduled for removal.
     * This role is intended to allow privileged applications an extended grace period to
     * adjust to the API changes before their access is cut-off as well.
     */
    SUNSETTED_API_FEATURES_ACCESS("sunsetted-api-features-access");

    private final String role;

    ApplicationAccessRole(String role) {
        this.role = checkNotNull(role);
    }

    public static ApplicationAccessRole from(String role) {
        Optional<ApplicationAccessRole> roleOptional = Arrays.stream(ApplicationAccessRole.values())
                .filter(applicationRole -> applicationRole.getRole().equals(role))
                .findFirst();

        return roleOptional.orElseThrow(
                () -> new IllegalArgumentException("Invalid role " + role)
        );
    }

    /**
     * Get the roles that all applications have access to by default
     */
    public static ImmutableSet<ApplicationAccessRole> getDefaultRoles() {
        return ImmutableSet.of(DEER_ACCESS);
    }

    public String getRole() {
        return role;
    }
}
