package uk.megaslice.delta;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.*;

import static java.util.Collections.*;

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
        Objects.requireNonNull(key, "key must not be null");

        return Optional.ofNullable(operations.get(key));
    }

    public boolean isEmpty() {
        return operations.isEmpty();
    }

    public Collection<T> apply(Iterable<T> items, NaturalKey<T, K> naturalKey) {
        Objects.requireNonNull(items, "items must not be null");
        Objects.requireNonNull(naturalKey, "naturalKey must not be null");

        Map<K, Operation<T>> remainingOps = new HashMap<>(operations);
        Map<K, T> itemsByKey = new HashMap<>();

        for (T item : items) {
            K key = keyOf(item, naturalKey);
            if (itemsByKey.containsKey(key)) {
                throw new DuplicateKeyException("Duplicate key in items: " + key);
            }
            applyUpdateOrUnchanged(remainingOps, itemsByKey, item, key);
        }

        applyInsertsAndDeletes(remainingOps, itemsByKey);

        return unmodifiableCollection(itemsByKey.values());
    }

    public Map<K, T> apply(Map<K, T> items) {
        Objects.requireNonNull(items, "items must not be null");

        Map<K, Operation<T>> remainingOps = new HashMap<>(operations);
        Map<K, T> itemsByKey = new HashMap<>();

        for (Map.Entry<K, T> entry : items.entrySet()) {
            K key = entry.getKey();
            T item = entry.getValue();
            requireNonNullKeyAndItem(key, item, "");

            applyUpdateOrUnchanged(remainingOps, itemsByKey, item, key);
        }

        applyInsertsAndDeletes(remainingOps, itemsByKey);

        return unmodifiableMap(itemsByKey);
    }

    private void applyUpdateOrUnchanged(Map<K, Operation<T>> remainingOps, Map<K, T> itemsByKey, T item, K key) {
        Operation<T> op = operations.get(key);
        if (op instanceof Operation.Update) {
            T updatedItem = ((Operation.Update<T>) op).after;
            itemsByKey.put(key, updatedItem);
            remainingOps.remove(key);
        } else {
            itemsByKey.put(key, item);
        }
    }

    private void applyInsertsAndDeletes(Map<K, Operation<T>> remainingOps, Map<K, T> itemsByKey) {
        for (Map.Entry<K, Operation<T>> entry : remainingOps.entrySet()) {
            Operation<T> op = entry.getValue();
            if (op instanceof Operation.Insert) {
                T itemToInsert = ((Operation.Insert<T>) op).item;
                if (itemsByKey.put(entry.getKey(), itemToInsert) != null) {
                    throw new DuplicateKeyException("Duplicate key in operations: " + entry.getKey());
                }
            } else {
                itemsByKey.remove(entry.getKey());
            }
        }
    }

    public Delta<T, K> combine(Delta<T, K> other) {
        return combine(other, Equivalence.defaultEquivalence());
    }

    public Delta<T, K> combine(Delta<T, K> other, Equivalence<T> equivalence) {
        Objects.requireNonNull(other, "other must not be null");
        Objects.requireNonNull(equivalence, "equivalence must not be null");

        if (isEmpty()) {
            return other;
        }
        if (other.isEmpty()) {
            return this;
        }

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

        Objects.requireNonNull(before, "before must not be null");
        Objects.requireNonNull(after, "after must not be null");
        Objects.requireNonNull(naturalKey, "naturalKey must not be null");
        Objects.requireNonNull(equivalence, "equivalence must not be null");

        if (before == after) {
            return empty();
        }

        Map<K, Operation<T>> operations = new HashMap<>();

        for (T beforeItem : before) {
            K key = keyOf(beforeItem, naturalKey);
            Object existing = operations.put(key, Operation.delete(beforeItem));
            if (existing != null) {
                throw new DuplicateKeyException("Duplicate key in 'before' items: " + key);
            }
        }

        Set<K> removed = new HashSet<>();
        for (T afterItem : after) {
            K key = keyOf(afterItem, naturalKey);
            if (removed.contains(key)) {
                throw new DuplicateKeyException("Duplicate key in 'after' items: " + key);
            }

            Operation<T> existing = operations.get(key);
            if (existing == null) {
                operations.put(key, Operation.insert(afterItem));
            } else if (existing instanceof Operation.Delete) {
                T beforeItem = ((Operation.Delete<T>) existing).item;

                if (equivalence.isEquivalent(beforeItem, afterItem)) {
                    operations.remove(key);
                    removed.add(key);
                } else {
                    operations.put(key, Operation.update(beforeItem, afterItem));
                }
            } else {
                throw new DuplicateKeyException("Duplicate key in 'after' items: " + key);
            }
        }

        return new Delta<>(operations);
    }

    public static <T, K> Delta<T, K> diff(Map<K, T> before,
                                          Map<K, T> after) {

        return diff(before, after, Equivalence.defaultEquivalence());
    }

    public static <T, K> Delta<T, K> diff(Map<K, T> before,
                                          Map<K, T> after,
                                          Equivalence<T> equivalence) {

        Objects.requireNonNull(before, "before must not be null");
        Objects.requireNonNull(after, "after must not be null");
        Objects.requireNonNull(equivalence, "equivalence must not be null");

        if (before == after) {
            return Delta.empty();
        }

        Map<K, Operation<T>> operations = new HashMap<>();

        for (Map.Entry<K, T> entry : before.entrySet()) {
            K key = entry.getKey();
            T beforeItem = entry.getValue();
            requireNonNullKeyAndItem(key, beforeItem, "before ");

            operations.put(key, Operation.delete(beforeItem));
        }

        for (Map.Entry<K, T> entry : after.entrySet()) {
            K key = entry.getKey();
            T afterItem = entry.getValue();
            requireNonNullKeyAndItem(key, afterItem, "after ");

            T beforeItem = before.get(key);
            if (beforeItem == null) {
                operations.put(key, Operation.insert(afterItem));
            } else if (equivalence.isEquivalent(beforeItem, afterItem)) {
                operations.remove(key);
            } else {
                operations.put(key, Operation.update(beforeItem, afterItem));
            }
        }

        return new Delta<>(operations);
    }

    private static <T, K> K keyOf(T item, NaturalKey<T, K> naturalKey) {
        Objects.requireNonNull(item, "item must not be null");

        K key = naturalKey.getNaturalKey(item);
        if (key == null) {
            throw new NullPointerException("null key for item: " + item);
        }
        return key;
    }

    private static <T, K> void requireNonNullKeyAndItem(K key, T item, String label) {
        if (key == null) {
            throw new NullPointerException(label + "key must not be null");
        }
        if (item == null) {
            throw new NullPointerException(label + "item must not be null");
        }
    }
}
