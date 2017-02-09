package org.atlasapi.system;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.application.sources.SourceIdCodec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SourcesModule {

    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final SourceIdCodec sourceIdCodec = SourceIdCodec.createWithCodec(idCodec);

    @Bean
    public SourcesController systemSourcesController() {
        return SourcesController.create(sourceIdCodec);
    }

}
