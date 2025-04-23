package io.appform.dropwizard.sharding.observers;

import io.appform.dropwizard.sharding.sharding.BucketKey;
import io.appform.dropwizard.sharding.sharding.ShardingKey;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.FieldNameConstants;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "simple_parents")
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@FieldNameConstants
public class BucketKeyAwareParent extends SimpleParent {

    @Column
    @BucketKey
    private int bucketKey;

    @Column
    @ShardingKey
    private String shardingKey;
}
