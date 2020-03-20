package uk.megaslice.delta;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.*;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

@ToString
@EqualsAndHashCode
public final class Delta<T, K> {

    private final Map<K, Operation<T>> operations;

    private Delta(Map<K, Operation<T>> operations) {
        this.operations = unmodifiableMap(operations);
    }

    public Map<K, Operation<T>> operations() {
        return operations;
    }

    public Optional<Operation<T>> get(K key) {
        return Optional.ofNullable(operations.get(key));
    }

    public boolean isEmpty() {
        return operations.isEmpty();
    }

    public Delta<T, K> combine(Delta<T, K> other) {
        return combine(other, Equivalence.defaultEquivalence());
    }

    public Delta<T, K> combine(Delta<T, K> other, Equivalence<T> equivalence) {
        Map<K, Operation<T>> combined = new HashMap<>(this.operations);

        for (Map.Entry<K, Operation<T>> entry : other.operations.entrySet()) {
            K key = entry.getKey();
            Operation<T> left = combined.get(key);
            Operation<T> right = entry.getValue();

            if (left == null) {
                combined.put(key, right);
            } else {
                Optional<Operation<T>> combinedOp = left.combine(right, equivalence);
                if (combinedOp.isPresent()) {
                    combined.put(key, combinedOp.get());
                } else {
                    combined.remove(key);
                }
            }
        }

        return new Delta<>(combined);
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

        return new Delta<>(operations);
    }
}
