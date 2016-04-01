package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Certificate;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

public class CertificateSerialization {

    public Certificate serialize(org.atlasapi.content.Certificate certificate) {
        if (certificate == null) {
            return null;
        }
        Certificate internal = new Certificate();

        internal.setClassification(certificate.classification());
        Country country = certificate.country();
        if (country != null) {
            internal.setCountryCode(country.code());
        }

        return internal;
    }

    public org.atlasapi.content.Certificate deserialize(Certificate cert) {
        if (cert == null) {
            return null;
        }

        return new org.atlasapi.content.Certificate(
                cert.getClassification(),
                Countries.fromCode(cert.getCountryCode())
        );
    }
}