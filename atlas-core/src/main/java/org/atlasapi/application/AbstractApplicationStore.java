package org.atlasapi.application;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.UUIDGenerator;
import com.metabroadcast.common.time.SystemClock;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public abstract class AbstractApplicationStore implements ApplicationStore {

    private final IdGenerator idGenerator;
    private final NumberToShortStringCodec idCodec;
    private final SystemClock clock;

    public AbstractApplicationStore(IdGenerator idGenerator,
            NumberToShortStringCodec idCodec, SystemClock clock) {
        this.idGenerator = idGenerator;
        this.idCodec = idCodec;
        this.clock = clock;
    }

    public AbstractApplicationStore(IdGenerator idGenerator,
            NumberToShortStringCodec idCodec) {
        this(idGenerator, idCodec, new SystemClock());
    }

    // For compatibility with 3.0
    private String generateSlug(Id id) {
        return "app-" + idCodec.encode(id.toBigInteger());
    }

    private String generateApiKey() {
        return new UUIDGenerator().generate();
    }

    abstract void doCreateApplication(Application application);

    abstract void doUpdateApplication(Application application);

    public final Application createApplication(Application application) {
        // Create requests do not have to post credentials or 
        // sources part of the object so ensure these exist
        ApplicationCredentials.Builder credentialsBuilder;
        if (application.getCredentials() != null) {
            credentialsBuilder = application.getCredentials().copy();
        } else {
            credentialsBuilder = ApplicationCredentials.builder();
        }
        credentialsBuilder = credentialsBuilder.withApiKey(generateApiKey());
        ApplicationSources sources = application.getSources();
        // If sources not given create an empty sources object
        if (sources == null) {
            sources = ApplicationSources.builder().build();
        }

        Id id = Id.valueOf(idGenerator.generateRaw());
        // Make sure any missing sources are populated
        Application created = application.copy()
                .withId(id)
                .withCreated(clock.now())
                .withSlug(generateSlug(id))
                .withCredentials(credentialsBuilder.build())
                .withSources(sources.copyWithMissingSourcesPopulated())
                .withRevoked(false)
                .build();

        doCreateApplication(created);
        return created;
    }

    @Override
    public final Application updateApplication(Application application) {
        Application updated = withGuaranteedSlug(application);
        // Make sure full list of sources present in application
        // May not be present is posted by non admin user
        updated = updated.copyWithSources(
                updated.getSources().copyWithMissingSourcesPopulated());
        doUpdateApplication(updated);
        return updated;
    }

    private Application withGuaranteedSlug(Application application) {
        Preconditions.checkNotNull(application);
        // Ensure slug is present for compatibility with 3.0
        if (Strings.isNullOrEmpty(application.getSlug())) {
            application = application.copy()
                    .withSlug(generateSlug(application.getId())).build();
        }
        return application;
    }

}
