package org.atlasapi;

import org.apache.commons.io.IOUtils;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.License;
import org.atlasapi.output.writers.LicenseWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class LicenseModule {

    @Value("classpath:license.txt")
    private org.springframework.core.io.Resource licenseText;

    @Bean
    EntityWriter<Object> licenseWriter() throws IOException {
        return new LicenseWriter(
                new License(IOUtils.toString(licenseText.getInputStream()))
        );
    }
}
