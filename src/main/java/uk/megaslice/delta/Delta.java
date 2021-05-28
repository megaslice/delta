package uk.megaslice.delta;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;

/**
 * Represents changes to a dataset in terms of insert, update and delete operations.
 *
 * Deltas can be created using the {@code diff} method.
 *
 * @param <T>  the type of dataset items
 * @param <K>  the type of the items' natural keys
 */
@ToString
@EqualsAndHashCode
public final class Delta<T, K> {

    private final Map<K, Operation<T>> operations;

    private Delta(Map<K, Operation<T>> operations) {
        this.operations = unmodifiableMap(operations);
    }

    /**
     * Returns the insert, update and delete operations of this delta.
     * @return  the operations of this delta
     */
    public Map<K, Operation<T>> operations() {
        return operations;
    }

    /**
     * Returns the insert operations of this delta.
     * @return  the insert operations of this delta
     */
    public Collection<Operation<T>> inserts() {
        return operations.values().stream()
                .filter(o -> o.type() == Operation.Type.INSERT)
                .collect(toList());
    }

    /**
     * Returns the inserted items of this delta.
     * @return  the inserted items of this delta
     */
    public Collection<T> insertedItems() {
        return operations.values().stream()
                .filter(o -> o.type() == Operation.Type.INSERT)
                .map(Operation::newItem)
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(toList());
    }

    /**
     * Returns the update operations of this delta.
     * @return  the update operations of this delta
     */
    public Collection<Operation<T>> updates() {
        return operations.values().stream()
                .filter(o -> o.type() == Operation.Type.UPDATE)
                .collect(toList());
    }

    /**
     * Returns the updated items of this delta.
     * @return  the updated items of this delta
     */
    public Collection<T> updatedItems() {
        return operations.values().stream()
                .filter(o -> o.type() == Operation.Type.UPDATE)
                .map(Operation::newItem)
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(toList());
    }

    /**
     * Returns the delete operations of this delta.
     * @return  the delete operations of this delta
     */
    public Collection<Operation<T>> deletes() {
        return operations.values().stream()
                .filter(o -> o.type() == Operation.Type.DELETE)
                .collect(toList());
    }

    /**
     * Returns the deleted items of this delta.
     * @return  the deleted items of this delta
     */
    public Collection<T> deletedItems() {
        return operations.values().stream()
                .filter(o -> o.type() == Operation.Type.DELETE)
                .map(Operation::oldItem)
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(toList());
    }

    /**
     * Retrieves an operation by the natural key of an item in the dataset.
     *
     * @param key  the natural key of an item affected by this delta
     * @return  a {@link java.util.Optional} containing the operation for the given key, or an empty
     *          {@link java.util.Optional} if no operation exists for the key
     * @throws  NullPointerException if {@code key} is null
     */
    public Optional<Operation<T>> get(K key) {
        Objects.requireNonNull(key, "key must not be null");

        return Optional.ofNullable(operations.get(key));
    }

    /**
     * Indicates whether this delta is empty.
     *
     * @return  true if this delta has no operations, false otherwise
     */
    public boolean isEmpty() {
        return operations.isEmpty();
    }

    /**
     * Applies this delta's inserts, updates and deletes to another dataset.
     *
     * @param items       the dataset to which this delta will be applied
     * @param naturalKey  a function to derive natural keys for dataset items
     * @return  a new collection containing items with this delta applied
     * @throws  DuplicateKeyException if {@code items} contains any items with the same natural key, or if
     *          this delta contains an insert with the same key as an item in the dataset
     * @throws  NullPointerException if {@code items} is null, any element in {@code items} is null,
     *          {@code naturalKey} is null or if {@code naturalKey} produces a null for any item
     */
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

    /**
     * Applies this delta's inserts, updates and deletes to another dataset.
     *
     * @param items  the dataset to which this delta will be applied
     * @return  a new collection containing items with this delta applied
     * @throws  DuplicateKeyException if this delta contains an insert with the same key as an item in the dataset
     * @throws  NullPointerException if {@code items} is null, or if any key or value in {@code items} is null
     */
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

    /**
     * Combines this delta with another delta, using a default item equivalence function.
     *
     * Applying a combined delta of A and B is equivalent to applying A, then B.
     *
     * @param other  the other delta with which to combine
     * @return  a new combined delta
     * @throws  InvalidCombination if operations for the same natural key in the deltas cannot be combined
     * @throws  NullPointerException if {@code other} is null
     * @see Operation#combine(Operation)
     */
    public Delta<T, K> combine(Delta<T, K> other) {
        return combine(other, Equivalence.defaultEquivalence());
    }

    /**
     * Combines this delta with another delta.
     *
     * Applying a combined delta of A and B is equivalent to applying A, then B.
     *
     * @param other        the other delta with which to combine
     * @param equivalence  a function to determine whether two dataset items are equivalent
     * @return  a new combined delta
     * @throws  InvalidCombination if operations for the same natural key in the deltas cannot be combined
     * @throws  NullPointerException if {@code other} is null or {@code equivalence} is null
     * @see Operation#combine(Operation, Equivalence)
     */
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

    /**
     * Returns an empty delta.
     *
     * @param <T>  the type of items in the delta
     * @param <K>  the type of the items' natural keys
     * @return  an empty delta
     */
    public static <T, K> Delta<T, K> empty() {
        @SuppressWarnings("unchecked")
        Delta<T, K> emptyDelta = (Delta<T, K>) EMPTY;
        return emptyDelta;
    }

    private static final Delta<Object, Object> EMPTY = new Delta<>(emptyMap());

    /**
     * Creates a delta from two datasets, using a default item equivalence function.
     *
     * @param before      the dataset before changes were made
     * @param after       the dataset after changes were made
     * @param naturalKey  a function to derive natural keys for dataset items in the datasets
     * @param <T>  the type of dataset items
     * @param <K>  the type of the items' natural keys
     * @return  a delta representing the difference between the two datasets in terms of inserts, updates and deletes
     * @throws  DuplicateKeyException if any items in {@code before} have the same natural key, or if any items
     *          in {@code after} have the same natural key
     * @throws  NullPointerException if {@code before} is null, {@code after} is null,
     *          any element in {@code before} or {@code after} is null, {@code naturalKey} is null
     *          or if {@code naturalKey} produces a null for any item
     */
    public static <T, K> Delta<T, K> diff(Iterable<T> before,
                                          Iterable<T> after,
                                          NaturalKey<T, K> naturalKey) {

        return diff(before, after, naturalKey, Equivalence.defaultEquivalence());
    }

    /**
     * Creates a delta from two datasets.
     *
     * @param before       the dataset before changes were made
     * @param after        the dataset after changes were made
     * @param naturalKey   a function to derive natural keys for dataset items in the datasets
     * @param equivalence  a function to determine whether two dataset items are equivalent
     * @param <T>  the type of dataset items
     * @param <K>  the type of the items' natural keys
     * @return  a delta representing the difference between the two datasets in terms of inserts, updates and deletes
     * @throws  DuplicateKeyException if any items in {@code before} have the same natural key, or if any items
     *          in {@code after} have the same natural key
     * @throws  NullPointerException if {@code before} is null, {@code after} is null,
     *          any element in {@code before} or {@code after} is null, {@code naturalKey} is null,
     *          {@code naturalKey} produces a null for any item or if {@code equivalence} is null
     */
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

    /**
     * Creates a delta from two datasets, using a default item equivalence function.
     *
     * @param before       the dataset before changes were made
     * @param after        the dataset after changes were made
     * @param <T>  the type of dataset items
     * @param <K>  the type of the items' natural keys
     * @return  a delta representing the difference between the two datasets in terms of inserts, updates and deletes
     * @throws  NullPointerException if {@code before} is null, {@code after} is null,
     *          or if any key or value in {@code before} or {@code after} is null
     */
    public static <T, K> Delta<T, K> diff(Map<K, T> before,
                                          Map<K, T> after) {

        return diff(before, after, Equivalence.defaultEquivalence());
    }

    /**
     * Creates a delta from two datasets.
     *
     * @param before       the dataset before changes were made
     * @param after        the dataset after changes were made
     * @param equivalence  a function to determine whether two dataset items are equivalent
     * @param <T>  the type of dataset items
     * @param <K>  the type of the items' natural keys
     * @return  a delta representing the difference between the two datasets in terms of inserts, updates and deletes
     * @throws  NullPointerException if {@code before} is null, {@code after} is null,
     *          any key or value in {@code before} or {@code after} is null, or if {@code equivalence} is null
     */
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
