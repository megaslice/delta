package uk.megaslice.delta;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Objects;
import java.util.Optional;

/**
 * An insert, delete or update operation.
 *
 * @param <T>  the type of a dataset item involved in an operation
 */
public abstract class Operation<T> {

    private Operation() {}

    /**
     * The type of an operation: insert, update or delete.
     */
    public enum Type {
        /** Indicates an insert operation. */
        INSERT,

        /** Indicates an update operation. */
        UPDATE,

        /** Indicates a delete operation. */
        DELETE
    }

    /**
     * Returns the type of this operation.
     * @return  the type of this operation
     */
    public abstract Type type();

    /**
     * Returns the previous value of the item affected by this operation. Not populated for inserts.
     * @return  the previous value of the item affected by in this operation
     */
    public Optional<T> oldItem() {
        return Optional.empty();
    }

    /**
     * Returns the current value of the item affected by this operation. Not populated for deletes.
     * @return  the current value of the item affected by this operation
     */
    public Optional<T> newItem() {
        return Optional.empty();
    }

    /**
     * Combines this operation with another operation, using a default item equivalence function.
     *
     * @param other  the other operation with which to combine
     * @return  an {@link java.util.Optional} containing the combined operation, or an empty {@link java.util.Optional}
     *          if the combination results in a no-op
     * @throws InvalidCombination if:
     *         <ul>
     *             <li>An insert is combined with another insert</li>
     *             <li>An update is combined with an insert</li>
     *             <li>A delete is combined with an update</li>
     *             <li>A delete is combined with a delete</li>
     *         </ul>
     * @throws NullPointerException if {@code other} is null
     */
    public Optional<Operation<T>> combine(Operation<T> other) {
        return combine(other, Equivalence.defaultEquivalence());
    }

    /**
     * Combines this operation with another operation.
     *
     * @param other  the other operation with which to combine
     * @param equivalence  a function to determine whether two dataset items are equivalent
     * @return  an {@link java.util.Optional} containing the combined operation, or an empty {@link java.util.Optional}
     *          if the combination results in a no-op
     * @throws InvalidCombination if:
     *         <ul>
     *             <li>An insert is combined with another insert</li>
     *             <li>An update is combined with an insert</li>
     *             <li>A delete is combined with an update</li>
     *             <li>A delete is combined with a delete</li>
     *         </ul>
     * @throws NullPointerException if {@code other} is null or {@code equivalence} is null
     */
    public abstract Optional<Operation<T>> combine(Operation<T> other, Equivalence<T> equivalence);

    /**
     * Creates an insert operation for a given item.
     *
     * @param item  the inserted item
     * @param <T>  the type of the inserted item
     * @return  a new insert operation containing the given item
     * @throws NullPointerException if {@code item} is null
     */
    public static <T> Operation<T> insert(T item) {
        Objects.requireNonNull(item, "item must not be null");
        return new Insert<>(item);
    }

    /**
     * Creates an update operation for a given item in its before and after states.
     *
     * @param before  the item before it was updated
     * @param after   the item after it was updated
     * @param <T>  the type of the updated item
     * @return  a new update operation containing the given item in its before and after states
     * @throws NullPointerException if {@code before} is null or {@code after} is null
     */
    public static <T> Operation<T> update(T before, T after) {
        Objects.requireNonNull(before, "before must not be null");
        Objects.requireNonNull(after, "after must not be null");
        return new Update<>(before, after);
    }

    /**
     * Creates a delete operation for a given item.
     *
     * @param item  the deleted item
     * @param <T>  the type of the deleted item
     * @return  a new delete operation containing the given item
     * @throws NullPointerException if {@code item} is null
     */
    public static <T> Operation<T> delete(T item) {
        Objects.requireNonNull(item, "item must not be null");
        return new Delete<>(item);
    }


    /**
     * An insert operation.
     *
     * @param <T>  the type of the inserted item
     */
    @ToString
    @EqualsAndHashCode(callSuper=false)
    static class Insert<T> extends Operation<T> {
        final T item;

        private Insert(T item) {
            this.item = item;
        }

        @Override
        public Operation.Type type() {
            return Type.INSERT;
        }

        @Override
        public Optional<T> newItem() {
            return Optional.of(item);
        }

        @Override
        public Optional<Operation<T>> combine(Operation<T> other, Equivalence<T> equivalence) {
            Objects.requireNonNull(other, "other must not be null");
            Objects.requireNonNull(equivalence, "equivalence must not be null");

            if (other instanceof Insert) {
                throw new InvalidCombination(Type.INSERT, Type.INSERT);
            } else if (other instanceof Update) {
                return Optional.of(new Insert<>(((Update<T>) other).after));
            } else if (other instanceof Delete) {
                return Optional.empty();
            } else {
                throw new IllegalStateException("Unsupported operation type: " + other.getClass());
            }
        }
    }

    /**
     * An update operation.
     *
     * @param <T>  the type of the updated item
     */
    @ToString
    @EqualsAndHashCode(callSuper=false)
    static class Update<T> extends Operation<T> {
        final T before;
        final T after;

        private Update(T before, T after) {
            this.before = before;
            this.after = after;
        }

        @Override
        public Operation.Type type() {
            return Type.UPDATE;
        }

        @Override
        public Optional<T> oldItem() {
            return Optional.of(before);
        }

        @Override
        public Optional<T> newItem() {
            return Optional.of(after);
        }

        @Override
        public Optional<Operation<T>> combine(Operation<T> other, Equivalence<T> equivalence) {
            Objects.requireNonNull(other, "other must not be null");
            Objects.requireNonNull(equivalence, "equivalence must not be null");

            if (other instanceof Insert) {
                throw new InvalidCombination(Type.UPDATE, Type.INSERT);
            } else if (other instanceof Update) {
                T otherAfter = ((Update<T>) other).after;
                return equivalence.isEquivalent(before, otherAfter) ?
                        Optional.empty() :
                        Optional.of(new Update<>(before, otherAfter));
            } else if (other instanceof Delete) {
                return Optional.of(other);
            } else {
                throw new IllegalStateException("Unsupported operation type: " + other.getClass());
            }
        }
    }

    /**
     * A delete operation.
     *
     * @param <T>  the type of the deleted item
     */
    @ToString
    @EqualsAndHashCode(callSuper=false)
    static class Delete<T> extends Operation<T> {
        final T item;

        private Delete(T item) {
            this.item = item;
        }

        @Override
        public Operation.Type type() {
            return Type.DELETE;
        }

        @Override
        public Optional<T> oldItem() {
            return Optional.of(item);
        }

        @Override
        public Optional<Operation<T>> combine(Operation<T> other, Equivalence<T> equivalence) {
            Objects.requireNonNull(other, "other must not be null");
            Objects.requireNonNull(equivalence, "equivalence must not be null");

            if (other instanceof Insert) {
                T otherItem = ((Insert<T>) other).item;
                return equivalence.isEquivalent(item, otherItem) ?
                        Optional.empty() :
                        Optional.of(new Update<>(item, otherItem));
            } else if (other instanceof Update) {
                throw new InvalidCombination(Type.DELETE, Type.UPDATE);
            } else if (other instanceof Delete) {
                throw new InvalidCombination(Type.DELETE, Type.DELETE);
            } else {
                throw new IllegalStateException("Unsupported operation type: " + other.getClass());
            }
        }
    }
}
