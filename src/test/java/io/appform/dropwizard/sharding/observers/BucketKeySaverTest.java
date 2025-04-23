package io.appform.dropwizard.sharding.observers;

import io.appform.dropwizard.sharding.BalancedDBShardingBundle;
import io.appform.dropwizard.sharding.BundleBasedTestBase;
import io.appform.dropwizard.sharding.DBShardingBundle;
import io.appform.dropwizard.sharding.DBShardingBundleBase;
import io.appform.dropwizard.sharding.MultiTenantBalancedDBShardingBundle;
import io.appform.dropwizard.sharding.config.MultiTenantShardedHibernateFactory;
import io.appform.dropwizard.sharding.config.ShardedHibernateFactory;
import io.appform.dropwizard.sharding.observers.bucket.BucketIdObserver;
import io.appform.dropwizard.sharding.observers.bucket.BucketIdSaver;
import io.appform.dropwizard.sharding.sharding.impl.ConsistentHashBucketIdExtractor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BucketKeySaverTest extends BundleBasedTestBase {

    @Override
    protected DBShardingBundleBase<TestConfig> getBundle() {
        return new BalancedDBShardingBundle<TestConfig>(BucketKeyAwareParent.class, SimpleChild.class) {

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

        val parentDao = bundle.createParentObjectDao(BucketKeyAwareParent.class);
        val childDao = bundle.createRelatedObjectDao(SimpleChild.class);

        val obj = new BucketKeyAwareParent();
        obj.setName("P1");
        obj.setShardingKey("P11010");
        val parent = parentDao.save(obj).orElse(null);
        assertNotNull(parent);
        val getParent = parentDao.get("P1");
        assertNotNull(getParent.get());
        assertNotEquals(0, getParent.get().getBucketKey());

//        val child = childDao.save(parent.getName(),
//                        new SimpleChild()
//                                .setParent(parent.getName())
//                                .setValue("CV1"))
//                .orElse(null);
//        assertNotNull(child);
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
