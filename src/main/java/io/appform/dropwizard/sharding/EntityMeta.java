package io.appform.dropwizard.sharding;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.lang.reflect.Field;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EntityMeta {
    private Field bucketKeyField;
    private Field shardingKeyField;
}
