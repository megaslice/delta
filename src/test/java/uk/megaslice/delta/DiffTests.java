package uk.megaslice.delta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.shuffle;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.junit.jupiter.api.Assertions.*;

class DiffTests {

    @Test
    void emptyInput() {
        Delta<Item, String> delta = Delta.diff(emptyList(), emptyList(), Item.naturalKey);

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

        Delta<Item, String> delta = Delta.diff(before, after, Item.naturalKey);

        assertEquals(Delta.<Item, String>empty(), delta);
        assertTrue(delta.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genItems")
    void sameInputReference(List<Item> items) {
        Delta<Item, String> delta = Delta.diff(items, items, Item.naturalKey);

        assertEquals(Delta.<Item, String>empty(), delta);
        assertTrue(delta.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genNonEmptyItems")
    void emptyBeforeNonEmptyAfter(List<Item> items) {
        Delta<Item, String> delta = Delta.diff(emptyList(), items, Item.naturalKey);

        assertTrue(delta.operations().values().stream().allMatch(op -> op.type() == Operation.Type.INSERT));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genNonEmptyItems")
    void nonEmptyBeforeEmptyAfter(List<Item> items) {
        Delta<Item, String> delta = Delta.diff(items, emptyList(), Item.naturalKey);

        assertTrue(delta.operations().values().stream().allMatch(op -> op.type() == Operation.Type.DELETE));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void addedItemsAndChangedKeys(Scenario scenario) {
        Delta<Item, String> delta = Delta.diff(scenario.before(), scenario.after(), Item.naturalKey);

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
        Delta<Item, String> delta = Delta.diff(scenario.before(), scenario.after(), Item.naturalKey);

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
        Delta<Item, String> delta = Delta.diff(scenario.before(), scenario.after(), Item.naturalKey);

        Set<Item> unchanged = new HashSet<>(scenario.unchanged);

        assertTrue(delta.operations().values().stream().noneMatch(op ->
                op.oldItem().map(unchanged::contains).orElse(false) ||
                        op.newItem().map(unchanged::contains).orElse(false)));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void removedItemsAndChangedKeys(Scenario scenario) {
        Delta<Item, String> delta = Delta.diff(scenario.before(), scenario.after(), Item.naturalKey);

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
