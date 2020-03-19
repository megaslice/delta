package uk.megaslice.delta;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.*;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

@ToString
@EqualsAndHashCode
public class Delta<T, K> {

    private final Map<K, Operation<T>> operations;

    private Delta(Map<K, Operation<T>> operations) {
        this.operations = operations;
    }

    public Map<K, Operation<T>> operations() {
        return operations;
    }

    public boolean isEmpty() {
        return operations.isEmpty();
    }

    public static <T, K> Delta<T, K> empty() {
        return new Delta<>(emptyMap());
    }

    public static <T, K> Delta<T, K> diff(Iterable<T> before,
                                          Iterable<T> after,
                                          NaturalKey<T, K> naturalKey) {

        return diff(before, after, naturalKey, Equivalence.defaultEquivalence());
    }

    public static <T, K> Delta<T, K> diff(Iterable<T> before,
                                          Iterable<T> after,
                                          NaturalKey<T, K> naturalKey,
                                          Equivalence<T> equivalence) {
        if (before == after) {
            return new Delta<>(emptyMap());
        }

        Map<K, Operation<T>> operations = new HashMap<>();

        for (T beforeItem : before) {
            K key = naturalKey.getNaturalKey(beforeItem);
            Object existing = operations.put(key, Operation.delete(beforeItem));
            if (existing != null) {
                throw new DuplicateKeyException("Duplicate key in 'before' items: " + key);
            }
        }

        Set<K> removed = new HashSet<>();
        for (T afterItem : after) {
            K key = naturalKey.getNaturalKey(afterItem);
            if (removed.contains(key)) {
                throw new DuplicateKeyException("Duplicate key in 'after' items: " + key);
            }

            Operation<T> existing = operations.get(key);
            if (existing == null) {
                operations.put(key, Operation.insert(afterItem));
            } else if (existing.newItem().isPresent()) {
                throw new DuplicateKeyException("Duplicate key in 'after' items: " + key);
            } else {
                @SuppressWarnings("OptionalGetWithoutIsPresent")
                T beforeItem = existing.oldItem().get();

                if (equivalence.isEquivalent(beforeItem, afterItem)) {
                    operations.remove(key);
                    removed.add(key);
                } else {
                    operations.put(key, Operation.update(beforeItem, afterItem));
                }
            }
        }

        return new Delta<>(unmodifiableMap(operations));
    }
}
