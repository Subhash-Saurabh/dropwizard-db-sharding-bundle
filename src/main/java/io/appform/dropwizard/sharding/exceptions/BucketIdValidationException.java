package io.appform.dropwizard.sharding.exceptions;

public class BucketIdValidationException extends RuntimeException {
    public BucketIdValidationException(int bucketIdCalculated, int bucketIdReceived) {
        super(String.format("Incorrect bucketId calculated %s and received %s", bucketIdCalculated, bucketIdReceived));
    }
}
