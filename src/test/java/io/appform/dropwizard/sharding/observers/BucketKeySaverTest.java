package io.appform.dropwizard.sharding.observers;

import io.appform.dropwizard.sharding.BalancedDBShardingBundle;
import io.appform.dropwizard.sharding.BundleBasedTestBase;
import io.appform.dropwizard.sharding.DBShardingBundleBase;
import io.appform.dropwizard.sharding.config.ShardedHibernateFactory;
import lombok.SneakyThrows;
import lombok.val;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Property;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BucketKeySaverTest extends BundleBasedTestBase {

    @Override
    protected DBShardingBundleBase<TestConfig> getBundle() {
        return new BalancedDBShardingBundle<TestConfig>(BucketKeyAwareParent.class, BucketKeyAwareChild.class) {

            @Override
            protected ShardedHibernateFactory getConfig(TestConfig config) {
                return testConfig.getShards();
            }
        };
    }

    @Test
    @SneakyThrows
    public void testObserverInvocationForBasicOps() {
        val bundle = createBundle();
        val shardingKey = "P11010";
        val name = "P11010";
        val childValue = "CV";

        val parentDao = bundle.createParentObjectDao(BucketKeyAwareParent.class);
        val childDao = bundle.createRelatedObjectDao(BucketKeyAwareChild.class);

        val obj = new BucketKeyAwareParent();
        obj.setName(shardingKey);
        obj.setShardingKey(shardingKey);
        val parent = parentDao.save(obj).orElse(null);
        assertNotNull(parent);
        val getParent = parentDao.get(shardingKey);
        assertNotNull(getParent.get());
        assertNotEquals(0, getParent.get().getBucketKey());

        val childObj = new BucketKeyAwareChild();
        childObj.setShardingKey(shardingKey);
        childObj.setParent(shardingKey);
        childObj.setValue(childValue);

        val child = childDao.save(shardingKey, childObj);
        assertNotNull(child);
        val getChild = childDao.select(shardingKey,  DetachedCriteria.forClass(BucketKeyAwareChild.class)
                        .add(Property.forName(BucketKeyAwareChild.Fields.shardingKey)
                                .eq(parent.getShardingKey())),
                0,
                Integer.MAX_VALUE);
        assertNotNull(getChild.get(0));
        assertNotEquals(0, getChild.get(0).getBucketKey());
    }

    private DBShardingBundleBase<TestConfig> createBundle() {
        val bundle = getBundle();
        bundle.initialize(bootstrap);
        bundle.initBundles(bootstrap);
        bundle.runBundles(testConfig, environment);
        bundle.run(testConfig, environment);
        return bundle;
    }


}
