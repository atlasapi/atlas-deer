package org.atlasapi.output.annotation;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.ExecutionException;

import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.representative.api.RepresentativeIdResponse;
import com.metabroadcast.representative.client.RepIdClient;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.atlasapi.application.ApplicationAccessRole.REP_ID_SERVICE;

public class RepIdAnnotation extends OutputAnnotation<Content> {

    private static Logger log = LoggerFactory.getLogger(RepIdAnnotation.class);

    private final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    RepIdClient repIdClient;
    LoadingCache<Long, String> appIdCache;

    public RepIdAnnotation(RepIdClient repIdClient) {
        this.repIdClient = repIdClient;
        this.appIdCache = CacheBuilder.newBuilder().build(
                new CacheLoader<Long, String>() {
                    @Override
                    public String load(Long id) {
                        return codec.encode(BigInteger.valueOf(id));
                    }
                });
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {


        String repId = null;

        if (!ctxt.getApplication().getAccessRoles().hasRole(REP_ID_SERVICE.getRole())) {
            throw new IllegalArgumentException(
                    "This application does not have access to the Representative ID service. "
                    + "Please ask MB about enabling access to the service.");
        }

        String appId;
        try {
            appId = appIdCache.get(ctxt.getApplication().getId());
            RepresentativeIdResponse repIdResponse;
            repIdResponse = repIdClient.getRepId(appId, entity.getId().longValue());
            repId = repIdResponse.getRepresentative().getId();
        } catch (ExecutionException e) {
            log.error(
                    "Rep ID could not be fetched because the app Long ID could not be encoded. app:{} id:{} {}",
                    ctxt.getApplication().getId(),
                    codec.encode(BigInteger.valueOf(entity.getId().longValue())),
                    e
            );
        }

        writer.writeField("rep_id", repId);
    }
}
