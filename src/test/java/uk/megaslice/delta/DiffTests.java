package uk.megaslice.delta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;

import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static org.junit.jupiter.api.Assertions.*;

class DiffTests {

    private Delta<Item, String> diff(Collection<Item> before, Collection<Item> after) {
        Delta<Item, String> delta = Delta.diff(before, after, Item.naturalKey);
        Delta<Item, String> deltaFromMap = Delta.diff(Item.toMap(before), Item.toMap(after));

        assertEquals(delta, deltaFromMap);

        return delta;
    }

    @Test
    void nullBefore() {
        assertThrows(NullPointerException.class, () -> Delta.diff(null, emptyList(), Item.naturalKey, Equivalence.defaultEquivalence()));
        assertThrows(NullPointerException.class, () -> Delta.diff(null, emptyList(), Item.naturalKey));
        assertThrows(NullPointerException.class, () -> Delta.diff(null, emptyMap(), Equivalence.defaultEquivalence()));
        assertThrows(NullPointerException.class, () -> Delta.diff(null, emptyMap()));
    }

    @Test
    void nullAfter() {
        assertThrows(NullPointerException.class, () -> Delta.diff(emptyList(), null, Item.naturalKey, Equivalence.defaultEquivalence()));
        assertThrows(NullPointerException.class, () -> Delta.diff(emptyList(), null, Item.naturalKey));
        assertThrows(NullPointerException.class, () -> Delta.diff(emptyMap(), null, Equivalence.defaultEquivalence()));
        assertThrows(NullPointerException.class, () -> Delta.diff(emptyMap(), null));
    }

    @Test
    void nullNaturalKey() {
        assertThrows(NullPointerException.class, () -> Delta.diff(emptyList(), emptyList(), null, Equivalence.defaultEquivalence()));
        assertThrows(NullPointerException.class, () -> Delta.diff(emptyList(), emptyList(), null));
    }

    @Test
    void nullEquivalence() {
        assertThrows(NullPointerException.class, () -> Delta.diff(emptyList(), emptyList(), Item.naturalKey, null));
        assertThrows(NullPointerException.class, () -> Delta.diff(emptyMap(), emptyMap(), null));
    }

    @Test
    void emptyInput() {
        Delta<Item, String> delta = diff(emptyList(), emptyList());

        assertEquals(Delta.<Item, String>empty(), delta);
        assertTrue(delta.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genItems")
    void equivalentInputs(List<Item> items) {
        List<Item> before = new ArrayList<>(items);
        List<Item> after = new ArrayList<>(items);
        shuffle(before, Generators.random);
        shuffle(after, Generators.random);

        Delta<Item, String> delta = diff(before, after);

        assertEquals(Delta.<Item, String>empty(), delta);
        assertTrue(delta.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genItems")
    void sameInputReference(List<Item> items) {
        Map<String, Item> itemMap = Item.toMap(items);
        Delta<Item, String> delta = Delta.diff(items, items, Item.naturalKey);
        Delta<Item, String> deltaFromMap = Delta.diff(itemMap, itemMap);

        assertEquals(delta, deltaFromMap);
        assertEquals(Delta.<Item, String>empty(), delta);
        assertTrue(delta.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genNonEmptyItems")
    void emptyBeforeNonEmptyAfter(List<Item> items) {
        Delta<Item, String> delta = diff(emptyList(), items);

        assertTrue(delta.operations().values().stream().allMatch(op -> op.type() == Operation.Type.INSERT));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genNonEmptyItems")
    void nonEmptyBeforeEmptyAfter(List<Item> items) {
        Delta<Item, String> delta = diff(items, emptyList());

        assertTrue(delta.operations().values().stream().allMatch(op -> op.type() == Operation.Type.DELETE));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void addedItemsAndChangedKeys(Scenario scenario) {
        Delta<Item, String> delta = diff(scenario.before(), scenario.after());

        Set<Operation<Item>> expectedInserts = concat(scenario.added.stream(), scenario.afterKeyChanged.stream())
                .map(Operation::insert)
                .collect(toSet());

        Set<Operation<Item>> actualInserts = delta.operations().values().stream()
                .filter(op -> op.type() == Operation.Type.INSERT)
                .collect(toSet());

        assertEquals(expectedInserts, actualInserts);
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void changedValues(Scenario scenario) {
        Delta<Item, String> delta = diff(scenario.before(), scenario.after());

        Set<Operation<Item>> expectedUpdates = new HashSet<>();
        for (int i = 0; i < scenario.beforeValueChanged.size(); i++) {
            Item before = scenario.beforeValueChanged.get(i);
            Item after = scenario.afterValueChanged.get(i);
            expectedUpdates.add(Operation.update(before, after));
        }

        Set<Operation<Item>> actualUpdates = delta.operations().values().stream()
                .filter(op -> op.type() == Operation.Type.UPDATE)
                .collect(toSet());

        assertEquals(expectedUpdates, actualUpdates);
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void unchangedItems(Scenario scenario) {
        Delta<Item, String> delta = diff(scenario.before(), scenario.after());

        Set<Item> unchanged = new HashSet<>(scenario.unchanged);

        assertTrue(delta.operations().values().stream().noneMatch(op ->
                op.oldItem().map(unchanged::contains).orElse(false) ||
                        op.newItem().map(unchanged::contains).orElse(false)));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void removedItemsAndChangedKeys(Scenario scenario) {
        Delta<Item, String> delta = diff(scenario.before(), scenario.after());

        Set<Operation<Item>> expectedDeletes = concat(scenario.removed.stream(), scenario.beforeKeyChanged.stream())
                .map(Operation::delete)
                .collect(toSet());

        Set<Operation<Item>> actualDeletes = delta.operations().values().stream()
                .filter(op -> op.type() == Operation.Type.DELETE)
                .collect(toSet());

        assertEquals(expectedDeletes, actualDeletes);
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void duplicateKeysInBeforeItems(Scenario scenario) {
        List<Item> beforeWithDuplicate = new ArrayList<>(scenario.before());
        beforeWithDuplicate.add(beforeWithDuplicate.get(0));
        shuffle(beforeWithDuplicate, Generators.random);

        assertThrows(DuplicateKeyException.class,
                () -> Delta.diff(beforeWithDuplicate, scenario.after(), Item.naturalKey));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void duplicateKeysInAfterItems(Scenario scenario) {
        List<Item> afterWithDuplicate = new ArrayList<>(scenario.after());
        afterWithDuplicate.add(afterWithDuplicate.get(0));
        shuffle(afterWithDuplicate, Generators.random);

        assertThrows(DuplicateKeyException.class,
                () -> Delta.diff(scenario.before(), afterWithDuplicate, Item.naturalKey));
    }
}
