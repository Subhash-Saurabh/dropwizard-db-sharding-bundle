package io.appform.dropwizard.sharding.observers.bucket;

import com.google.common.base.Preconditions;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

@Slf4j
public class BucketIdObserver extends TransactionObserver {
    private final BucketIdSaver bucketIdSaver;

    public BucketIdObserver(final BucketIdSaver bucketIdSaver) {
        super(null);
        this.bucketIdSaver = bucketIdSaver;
    }

    @Override
    public <T> T execute(TransactionExecutionContext context, Supplier<T> supplier) {
        context.getOpContext().visit(this.bucketIdSaver);
        return proceed(context, supplier);
    }

}