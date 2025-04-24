package io.appform.dropwizard.sharding.observers;

import io.appform.dropwizard.sharding.sharding.BucketKey;
import io.appform.dropwizard.sharding.sharding.ShardingKey;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.FieldNameConstants;
import lombok.experimental.SuperBuilder;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "simple_children")
@FieldNameConstants
@Getter
@Setter
@ToString(callSuper = true)
@RequiredArgsConstructor
public class BucketKeyAwareChild extends SimpleChild {
    @Column
    @ShardingKey
    private String shardingKey;

    @Column
    @BucketKey
    private int bucketKey;
}
