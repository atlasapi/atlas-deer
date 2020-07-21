package org.atlasapi.system.bootstrap;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.organisation.OrganisationWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class OrganisationBoostrapController {

    private final OrganisationResolver resolver;
    private final OrganisationWriter writer;
    private final NumberToShortStringCodec idCodec;

    public OrganisationBoostrapController(OrganisationResolver resolver, OrganisationWriter writer) {
        this(resolver,writer, SubstitutionTableNumberCodec.lowerCaseOnly());
    }

    @VisibleForTesting
    OrganisationBoostrapController(OrganisationResolver resolver, OrganisationWriter writer,
            NumberToShortStringCodec idCodec) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.idCodec = checkNotNull(idCodec);
    }

    @RequestMapping(value = "/system/bootstrap/organisation/{id}", method = RequestMethod.POST)
    public void bootstrapOrganisation(@PathVariable("id") String encodedId, HttpServletResponse resp)
            throws IOException {
        Id id = Id.valueOf(idCodec.decode(encodedId).longValue());
        executeBootstrap(resp, id);
    }

    @RequestMapping(value = "/system/bootstrap/organisation/numeric/{id}", method = RequestMethod.POST)
    public void bootstrapOrganisation(@PathVariable("id") Long numeric, HttpServletResponse resp)
            throws IOException {
        Id id = Id.valueOf(numeric);
        executeBootstrap(resp, id);
    }

    private void executeBootstrap(HttpServletResponse resp, Id id) throws IOException {
        ListenableFuture<Resolved<Organisation>> future = resolver.resolveIds(ImmutableList.of(id));

        Resolved<Organisation> resolved = Futures.get(future, IOException.class);
        if (resolved.getResources().isEmpty()) {
            resp.sendError(HttpStatus.NOT_FOUND.value());
            return;
        }
        for (Organisation organisation : resolved.getResources()) {
            writer.write(organisation);
        }
        resp.setStatus(HttpStatus.OK.value());
        resp.setContentLength(0);
    }
}
