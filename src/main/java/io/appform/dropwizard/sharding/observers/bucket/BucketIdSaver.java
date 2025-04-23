package io.appform.dropwizard.sharding.observers.bucket;

import com.google.common.base.Preconditions;
import io.appform.dropwizard.sharding.EntityMeta;
import io.appform.dropwizard.sharding.dao.operations.Count;
import io.appform.dropwizard.sharding.dao.operations.CountByQuerySpec;
import io.appform.dropwizard.sharding.dao.operations.Get;
import io.appform.dropwizard.sharding.dao.operations.GetAndUpdate;
import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.RunInSession;
import io.appform.dropwizard.sharding.dao.operations.RunWithCriteria;
import io.appform.dropwizard.sharding.dao.operations.Save;
import io.appform.dropwizard.sharding.dao.operations.SaveAll;
import io.appform.dropwizard.sharding.dao.operations.Select;
import io.appform.dropwizard.sharding.dao.operations.SelectAndUpdate;
import io.appform.dropwizard.sharding.dao.operations.UpdateAll;
import io.appform.dropwizard.sharding.dao.operations.UpdateByQuery;
import io.appform.dropwizard.sharding.dao.operations.UpdateWithScroll;
import io.appform.dropwizard.sharding.dao.operations.lockedcontext.LockAndExecute;
import io.appform.dropwizard.sharding.dao.operations.lookupdao.CreateOrUpdateByLookupKey;
import io.appform.dropwizard.sharding.dao.operations.lookupdao.DeleteByLookupKey;
import io.appform.dropwizard.sharding.dao.operations.lookupdao.GetAndUpdateByLookupKey;
import io.appform.dropwizard.sharding.dao.operations.lookupdao.GetByLookupKey;
import io.appform.dropwizard.sharding.dao.operations.lookupdao.readonlycontext.ReadOnlyForLookupDao;
import io.appform.dropwizard.sharding.dao.operations.relationaldao.CreateOrUpdate;
import io.appform.dropwizard.sharding.dao.operations.relationaldao.CreateOrUpdateInLockedContext;
import io.appform.dropwizard.sharding.dao.operations.relationaldao.readonlycontext.ReadOnlyForRelationalDao;
import io.appform.dropwizard.sharding.exceptions.BucketIdValidationException;
import io.appform.dropwizard.sharding.sharding.BucketIdExtractor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class BucketIdSaver implements OpContext.OpContextVisitor<Void> {
    private static final String OPERATION_NOT_SUPPORTED = " operation not supported";
    private final BucketIdExtractor<String> bucketIdExtractor;
    private final String tenantId;
    private final Map<String, EntityMeta> initialisedEntityMeta;

    public BucketIdSaver(final BucketIdExtractor<String> bucketIdExtractor,
                         final String tenantId,
                         final Map<String, EntityMeta> initialisedEntityMeta) {
        Preconditions.checkArgument(!Objects.isNull(bucketIdExtractor), "bucketId Extractor must not be null");
        Preconditions.checkArgument(!StringUtils.isEmpty(tenantId), "tenantId must not be empty");
        this.bucketIdExtractor = bucketIdExtractor;
        this.tenantId = tenantId;
        this.initialisedEntityMeta = initialisedEntityMeta;
    }

    @Override
    public Void visit(Count count) {
        return null;
    }

    @Override
    public Void visit(CountByQuerySpec countByQuerySpec) {
        return null;
    }

    @Override
    public <T, R> Void visit(Get<T, R> opContext) {
        return null;
    }

    @Override
    public <T> Void visit(GetAndUpdate<T> opContext) {
        validateIncomingBucketId(opContext.getUpdater());
        return null;
    }

    @Override
    public <T, R> Void visit(GetByLookupKey<T, R> getByLookupKey) {
        return null;
    }

    @Override
    public <T> Void visit(GetAndUpdateByLookupKey<T> getAndUpdateByLookupKey) {
        validateIncomingBucketId(getAndUpdateByLookupKey.getUpdater());
        return null;
    }

    @Override
    public <T> Void visit(ReadOnlyForLookupDao<T> readOnlyForLookupDao) {
        return null;
    }

    @Override
    public <T> Void visit(ReadOnlyForRelationalDao<T> readOnlyForRelationalDao) {
        return null;
    }

    @Override
    public <T> Void visit(LockAndExecute<T> opContext) {
        val contextMode = opContext.getMode();
        switch (contextMode) {
            case READ:
                return null;
            case INSERT:
                val oldSaver = opContext.getSaver();
                opContext.setSaver(oldSaver.compose((T entity) -> {
                    addBucketId(entity);
                    return entity;
                }));
                break;
            default:
                throw new UnsupportedOperationException(contextMode + OPERATION_NOT_SUPPORTED);
        }
        return null;
    }

    @Override
    public Void visit(UpdateByQuery updateByQuery) {
        validateIncomingBucketId(updateByQuery.getUpdater());
        return null;
    }

    @Override
    public <T> Void visit(UpdateWithScroll<T> updateWithScroll) {
        validateIncomingBucketId(updateWithScroll.getUpdater());
        return null;
    }

    @Override
    public <T> Void visit(UpdateAll<T> updateAll) {
        validateIncomingBucketId(updateAll.getUpdater());
        return null;
    }

    @Override
    public <T> Void visit(SelectAndUpdate<T> selectAndUpdate) {
        validateIncomingBucketId(selectAndUpdate.getUpdater());
        return null;
    }

    @Override
    public <T> Void visit(RunInSession<T> runInSession) {
        return null;
    }

    @Override
    public <T> Void visit(RunWithCriteria<T> runWithCriteria) {
        return null;
    }

    @Override
    public Void visit(DeleteByLookupKey deleteByLookupKey) {
        return null;
    }

    @Override
    public <T, R> Void visit(Save<T, R> opContext) {
        val oldSaver = opContext.getSaver();
        opContext.setSaver((T t) -> {
            addBucketId(opContext.getEntity());
            return oldSaver.apply(opContext.getEntity());
        });
        return null;
    }

    @Override
    public <T> Void visit(SaveAll<T> opContext) {
        val oldSaver = opContext.getSaver();
        val beforeExecute = oldSaver.compose((Collection<T> entities) -> {
            opContext.getEntities().forEach(this::addBucketId);
            return entities;
        });
        opContext.setSaver(beforeExecute);
        return null;
    }

    @Override
    public <T> Void visit(CreateOrUpdateByLookupKey<T> createOrUpdateByLookupKey) {
        T result = createOrUpdateByLookupKey.getGetLockedForWrite().apply(createOrUpdateByLookupKey.getId());
        if (result != null) {
            validateIncomingBucketId(createOrUpdateByLookupKey.getMutator().apply(result));
        }
        val oldSaver = createOrUpdateByLookupKey.getSaver();
        createOrUpdateByLookupKey.setSaver((T t) -> {
            addBucketId(createOrUpdateByLookupKey.getEntityGenerator().get());
            return oldSaver.apply(createOrUpdateByLookupKey.getEntityGenerator().get());
        });
        return null;
    }

    @Override
    public <T> Void visit(CreateOrUpdate<T> createOrUpdate) {
        val oldMutator = createOrUpdate.getMutator();
        createOrUpdate.setMutator(result -> {
            if (result != null) {
                T value = oldMutator.apply(result);
                addBucketId(value);
                return value;
            }
            return null;
        });

        val oldSaver = createOrUpdate.getSaver();
        createOrUpdate.setSaver((T result) -> {
            addBucketId(result);
            return oldSaver.apply(result);
        });
        return null;
    }

    @Override
    public <T, U> Void visit(CreateOrUpdateInLockedContext<T, U> createOrUpdateInLockedContext) {
        validateIncomingBucketId(createOrUpdateInLockedContext.getUpdater());
        val oldSaver = createOrUpdateInLockedContext.getSaver();
        createOrUpdateInLockedContext.setSaver((T t) -> {
            addBucketId(createOrUpdateInLockedContext.getLockedEntity());
            return oldSaver.apply((T) createOrUpdateInLockedContext.getLockedEntity());
        });
        return null;
    }

    @Override
    public <T, R> Void visit(Select<T, R> select) {
        return null;
    }

    private <T> void validateIncomingBucketId(T entity) {
        if(Objects.isNull(entity)) {
            return;
        }
        val entitymeta = initialisedEntityMeta.get(entity.getClass().getName());
        val bucketKeyField = entitymeta.getBucketKeyField();
        val shardingKeyField = entitymeta.getShardingKeyField();
        if (Objects.isNull(bucketKeyField) || Objects.isNull(shardingKeyField)) {
            return;
        }

        val bucketId = (Integer) resolveFieldData(entity, bucketKeyField);
        val shardingKey = (String) resolveFieldData(entity, shardingKeyField).toString();
        val expectedBucketId = bucketIdExtractor.bucketId(this.tenantId, shardingKey);
        if(expectedBucketId != bucketId){
            throw new BucketIdValidationException(expectedBucketId, bucketId);
        }
    }

    private <T> void addBucketId(T entity) {
        if(Objects.isNull(entity)) {
            return;
        }

        val entitymeta = initialisedEntityMeta.get(entity.getClass().getName());
        val bucketKeyField = entitymeta.getBucketKeyField();
        val shardingKeyField = entitymeta.getShardingKeyField();
        if (Objects.isNull(bucketKeyField) || Objects.isNull(shardingKeyField)) {
            return;
        }
        val shardingKey = (String) resolveFieldData(entity, shardingKeyField).toString();
        val bucketId = this.bucketIdExtractor.bucketId(this.tenantId, shardingKey);

        try {
            bucketKeyField.setAccessible(true);
            bucketKeyField.set(entity, bucketId);
        } catch (IllegalAccessException e) {
            log.error("Error setting field {}", bucketKeyField.getName(), e);
            throw new IllegalArgumentException(e);
        }
    }

    private <T> Object resolveFieldData(T entity, Field field) {
        try {
            field.setAccessible(true);
            return field.get(entity);
        } catch (IllegalAccessException e) {
            log.error("Error resolving field {}", field.getName(), e);
            throw new IllegalArgumentException(e);
        }
    }
}
