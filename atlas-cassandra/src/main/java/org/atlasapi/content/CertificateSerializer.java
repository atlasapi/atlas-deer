package org.atlasapi.content;

import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;

import com.metabroadcast.common.intl.Countries;

public class CertificateSerializer {

    public CommonProtos.Certificate serialize(Certificate certificate) {
        return CommonProtos.Certificate.newBuilder()
            .setClassification(certificate.classification())
            .setCountry(certificate.country().code()).build();
    }

    public Certificate deserialize(CommonProtos.Certificate cert) {
        return new Certificate(cert.getClassification(), 
            Countries.fromCode(cert.getCountry()));
    }
    
}
