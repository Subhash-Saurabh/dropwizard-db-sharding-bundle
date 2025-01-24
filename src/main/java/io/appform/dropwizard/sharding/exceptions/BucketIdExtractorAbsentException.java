package io.appform.dropwizard.sharding.exceptions;

public class BucketIdExtractorAbsentException extends RuntimeException {
    public BucketIdExtractorAbsentException(Class<?> entityClass) {
        super(String.format("BucketId Extractor not present for entityClass %s", entityClass.getName()));
    }
}
