package org.atlasapi.application;

import java.io.UnsupportedEncodingException;

import javax.mail.MessagingException;

import org.atlasapi.application.SourceStatus.SourceState;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.InvalidTransitionException;
import org.atlasapi.output.LicenseNotAcceptedException;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.ResourceForbiddenException;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.time.Clock;

import com.google.common.base.Optional;
import org.elasticsearch.common.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceRequestManager {

    private final SourceRequestStore sourceRequestStore;
    private final ApplicationStore applicationStore;
    private final IdGenerator idGenerator;
    private final Clock clock;

    private static Logger log = LoggerFactory.getLogger(SourceRequestManager.class);

    public SourceRequestManager(
            SourceRequestStore sourceRequestStore,
            ApplicationStore applicationStore,
            IdGenerator idGenerator,
            Clock clock
    ) {
        this.sourceRequestStore = sourceRequestStore;
        this.applicationStore = applicationStore;
        this.idGenerator = idGenerator;
        this.clock = clock;
    }

    public SourceRequest createOrUpdateRequest(Publisher source, UsageType usageType,
            Id applicationId, String applicationUrl, String email, String reason,
            boolean licenseAccepted)
            throws LicenseNotAcceptedException, InvalidTransitionException {
        Optional<SourceRequest> existing = sourceRequestStore.getBy(applicationId, source);
        if (!licenseAccepted) {
            throw new LicenseNotAcceptedException();
        }

        Preconditions.checkNotNull(source);
        Preconditions.checkNotNull(applicationId);
        Preconditions.checkNotNull(usageType);
        if (existing.isPresent()) {
            return updateSourceRequest(existing.get(), usageType,
                    applicationUrl, email, reason
            );
        } else {
            Application application = applicationStore.applicationFor(applicationId).get();
            SourceState appSourceState = application.getSources()
                    .readStatusOrDefault(source)
                    .getState();
            if (appSourceState.equals(SourceState.UNAVAILABLE)) {
                return createSourceRequest(source, usageType,
                        applicationId, applicationUrl, email, reason
                );
            } else if (appSourceState.equals(SourceState.ENABLEABLE)) {
                return createAndApproveSourceRequest(source, usageType,
                        applicationId, applicationUrl, email, reason
                );
            } else {
                // Not allowed source status change
                String message = "";
                if (appSourceState.equals(SourceState.REVOKED)) {
                    message = "Cannot process source request as source has been revoked";
                } else {
                    message = "Cannot process source request for a source with "
                            + "a status of " + appSourceState.toString();
                }
                throw new InvalidTransitionException(message);
            }
        }
    }

    private SourceRequest createSourceRequest(
            Publisher source,
            UsageType usageType,
            Id applicationId,
            String applicationUrl,
            String email,
            String reason
    ) {
        SourceRequest sourceRequest = SourceRequest.builder()
                .withId(Id.valueOf(idGenerator.generateRaw()))
                .withAppId(applicationId)
                .withAppUrl(applicationUrl)
                .withApproved(false)
                .withEmail(email)
                .withReason(reason)
                .withSource(source)
                .withUsageType(usageType)
                .withRequestedAt(clock.now())
                .build();
        sourceRequestStore.store(sourceRequest);
        Application existing = applicationStore.applicationFor(sourceRequest.getAppId()).get();
        applicationStore.updateApplication(
                existing.copyWithReadSourceState(sourceRequest.getSource(), SourceState.REQUESTED));

        log.info(
                "Requesting source access for application {}, {}",
                existing.getId(),
                sourceRequest
        );

        return sourceRequest;
    }

    // auto approve if not a source requiring manual approval
    private SourceRequest createAndApproveSourceRequest(
            Publisher source,
            UsageType usageType,
            Id applicationId,
            String applicationUrl,
            String email,
            String reason
    ) {
        SourceRequest sourceRequest = SourceRequest.builder()
                .withId(Id.valueOf(idGenerator.generateRaw()))
                .withAppId(applicationId)
                .withAppUrl(applicationUrl)
                .withEmail(email)
                .withReason(reason)
                .withSource(source)
                .withUsageType(usageType)
                .withRequestedAt(clock.now())
                .withApprovedAt(clock.now())
                .withLicenseAccepted(true)
                .build();
        sourceRequestStore.store(sourceRequest);
        Application existing = applicationStore.applicationFor(sourceRequest.getAppId()).get();
        applicationStore.updateApplication(
                existing
                        .copyWithReadSourceState(sourceRequest.getSource(), SourceState.AVAILABLE)
                        .copyWithSourceEnabled(sourceRequest.getSource())
        );
        return sourceRequest;
    }

    private SourceRequest updateSourceRequest(
            SourceRequest existing,
            UsageType usageType,
            String applicationUrl,
            String email,
            String reason
    ) {
        SourceRequest sourceRequest = existing.copy()
                .withAppUrl(applicationUrl)
                .withEmail(email)
                .withReason(reason)
                .withUsageType(usageType)
                .build();
        sourceRequestStore.store(sourceRequest);
        return sourceRequest;
    }

    /**
     * Approve source request and change source status on app to available Must be admin of source
     * to approve
     *
     * @param id
     * @throws NotFoundException
     * @throws ResourceForbiddenException
     * @throws MessagingException
     * @throws UnsupportedEncodingException
     */
    public void approveSourceRequest(Id id, User approvingUser)
            throws NotFoundException, ResourceForbiddenException, UnsupportedEncodingException,
            MessagingException {
        Optional<SourceRequest> sourceRequest = sourceRequestStore.sourceRequestFor(id);
        if (!sourceRequest.isPresent()) {
            throw new NotFoundException(id);
        }
        if (!approvingUser.is(Role.ADMIN)
                && !approvingUser.getSources().contains(sourceRequest.get().getSource())) {
            throw new ResourceForbiddenException();
        }
        Application existing = applicationStore.applicationFor(sourceRequest.get().getAppId())
                .get();
        applicationStore.updateApplication(
                existing.copyWithReadSourceState(
                        sourceRequest.get().getSource(),
                        SourceState.AVAILABLE
                ));
        SourceRequest approved = sourceRequest.get().copy().withApprovedAt(clock.now()).build();
        sourceRequestStore.store(approved);

        log.info(
                "Approving source request for application {}, {}",
                existing.getId(),
                approved
        );
    }
}
