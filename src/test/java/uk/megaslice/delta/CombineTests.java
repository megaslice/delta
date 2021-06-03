package uk.megaslice.delta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.megaslice.delta.Generators.copyItemWithRandomValue;
import static uk.megaslice.delta.Generators.genItem;

class CombineTests {

    @Test
    void nullDelta() {
        assertThrows(NullPointerException.class, () -> Delta.empty().combine(null));
        assertThrows(NullPointerException.class, () -> Delta.empty().combine(null, Equivalence.defaultEquivalence()));
    }

    @Test
    void nullEquivalence() {
        assertThrows(NullPointerException.class, () -> Delta.empty().combine(Delta.empty(), (Equivalence<Object>) null));
    }

    @Test
    void nullEssence() {
        assertThrows(NullPointerException.class, () -> Delta.empty().combine(Delta.empty(), (Essence<Object, Object>) null));
    }

    @Test
    void emptyWithEmpty() {
        assertEquals(Delta.empty(), Delta.empty().combine(Delta.empty()));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genDelta")
    void emptyWithNonEmpty(Delta<Item, String> delta) {
        assertEquals(delta, Delta.<Item, String>empty().combine(delta));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genDelta")
    void nonEmptyWithEmpty(Delta<Item, String> delta) {
        assertEquals(delta, delta.combine(Delta.empty()));
    }

    @Test
    void insertWithInsert() {
        Delta<Item, String> insertDelta = Delta.diff(emptySet(), singleton(genItem()), Item.naturalKey);

        InvalidCombination ex = assertThrows(InvalidCombination.class, () -> insertDelta.combine(insertDelta));
        assertEquals("Can't combine insert with insert", ex.getMessage());
    }

    @Test
    void insertWithUpdate() {
        Item item = genItem();
        Item updatedItem = copyItemWithRandomValue(item);
        Delta<Item, String> insertDelta = Delta.diff(emptySet(), singleton(item), Item.naturalKey);
        Delta<Item, String> updateDelta = Delta.diff(singleton(item), singleton(updatedItem), Item.naturalKey);

        Delta<Item, String> combinedDelta = insertDelta.combine(updateDelta);

        Operation<Item> op = combinedDelta.operations().values().iterator().next();
        assertEquals(Operation.Type.INSERT, op.type());
        assertEquals(Optional.empty(), op.oldItem());
        assertEquals(Optional.of(updatedItem), op.newItem());
    }

    @Test
    void insertWithDelete() {
        Item item = genItem();
        Delta<Item, String> insertDelta = Delta.diff(emptySet(), singleton(item), Item.naturalKey);
        Delta<Item, String> deleteDelta = Delta.diff(singleton(item), emptySet(), Item.naturalKey);

        Delta<Item, String> combinedDelta = insertDelta.combine(deleteDelta);

        assertEquals(Delta.<Item, String>empty(), combinedDelta);
    }

    @Test
    void updateWithInsert() {
        Item item = genItem();
        Item updatedItem = copyItemWithRandomValue(item);
        Delta<Item, String> updateDelta = Delta.diff(singleton(item), singleton(updatedItem), Item.naturalKey);
        Delta<Item, String> insertDelta = Delta.diff(emptySet(), singleton(updatedItem), Item.naturalKey);

        InvalidCombination ex = assertThrows(InvalidCombination.class, () -> updateDelta.combine(insertDelta));
        assertEquals("Can't combine update with insert", ex.getMessage());
    }

    @Test
    void updateWithUpdateWhenEquivalent() {
        Item item = genItem();
        Item updatedItem = copyItemWithRandomValue(item);
        Delta<Item, String> updateDelta1 = Delta.diff(singleton(item), singleton(updatedItem), Item.naturalKey);
        Delta<Item, String> updateDelta2 = Delta.diff(singleton(updatedItem), singleton(item), Item.naturalKey);

        Delta<Item, String> combinedDelta = updateDelta1.combine(updateDelta2);

        assertEquals(Delta.<Item, String>empty(), combinedDelta);
    }

    @Test
    void updateWithUpdateWhenDifferent() {
        Item item = genItem();
        Item updatedItem1 = copyItemWithRandomValue(item);
        Item updatedItem2 = copyItemWithRandomValue(item);
        Delta<Item, String> updateDelta1 = Delta.diff(singleton(item), singleton(updatedItem1), Item.naturalKey);
        Delta<Item, String> updateDelta2 = Delta.diff(singleton(updatedItem1), singleton(updatedItem2), Item.naturalKey);

        Delta<Item, String> combinedDelta = updateDelta1.combine(updateDelta2);

        Operation<Item> op = combinedDelta.operations().values().iterator().next();
        assertEquals(Operation.Type.UPDATE, op.type());
        assertEquals(Optional.of(item), op.oldItem());
        assertEquals(Optional.of(updatedItem2), op.newItem());
    }

    @Test
    void updateWithDelete() {
        Item item = genItem();
        Item updatedItem = copyItemWithRandomValue(item);
        Delta<Item, String> updateDelta = Delta.diff(singleton(item), singleton(updatedItem), Item.naturalKey);
        Delta<Item, String> deleteDelta = Delta.diff(singleton(updatedItem), emptySet(), Item.naturalKey);

        Delta<Item, String> combinedDelta = updateDelta.combine(deleteDelta);

        Operation<Item> op = combinedDelta.operations().values().iterator().next();
        assertEquals(Operation.Type.DELETE, op.type());
        assertEquals(Optional.of(updatedItem), op.oldItem());
        assertEquals(Optional.empty(), op.newItem());
    }

    @Test
    void deleteWithInsertWhenEquivalent() {
        Item item = genItem();
        Delta<Item, String> deleteDelta = Delta.diff(singleton(item), emptySet(), Item.naturalKey);
        Delta<Item, String> insertDelta = Delta.diff(emptySet(), singleton(item), Item.naturalKey);

        Delta<Item, String> combinedDelta = deleteDelta.combine(insertDelta);

        assertEquals(Delta.<Item, String>empty(), combinedDelta);
    }

    @Test
    void deleteWithInsertWhenDifferent() {
        Item item = genItem();
        Item updatedItem = copyItemWithRandomValue(item);
        Delta<Item, String> deleteDelta = Delta.diff(singleton(item), emptySet(), Item.naturalKey);
        Delta<Item, String> insertDelta = Delta.diff(emptySet(), singleton(updatedItem), Item.naturalKey);

        Delta<Item, String> combinedDelta = deleteDelta.combine(insertDelta);

        Operation<Item> op = combinedDelta.operations().values().iterator().next();
        assertEquals(Operation.Type.UPDATE, op.type());
        assertEquals(Optional.of(item), op.oldItem());
        assertEquals(Optional.of(updatedItem), op.newItem());
    }

    @Test
    void deleteWithUpdate() {
        Item item = genItem();
        Item updatedItem = copyItemWithRandomValue(item);
        Delta<Item, String> deleteDelta = Delta.diff(singleton(item), emptySet(), Item.naturalKey);
        Delta<Item, String> updateDelta = Delta.diff(singleton(item), singleton(updatedItem), Item.naturalKey);

        InvalidCombination ex = assertThrows(InvalidCombination.class, () -> deleteDelta.combine(updateDelta));
        assertEquals("Can't combine delete with update", ex.getMessage());
    }

    @Test
    void deleteWithDelete() {
        Item item = genItem();
        Delta<Item, String> deleteDelta = Delta.diff(singleton(item), emptySet(), Item.naturalKey);

        InvalidCombination ex = assertThrows(InvalidCombination.class, () -> deleteDelta.combine(deleteDelta));
        assertEquals("Can't combine delete with delete", ex.getMessage());
    }
}
