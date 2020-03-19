package uk.megaslice.delta;

import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.*;

@ToString
class Scenario {
    List<Item> added = emptyList();
    List<Item> unchanged = emptyList();
    List<Item> removed = emptyList();
    List<Item> beforeValueChanged = emptyList();
    List<Item> afterValueChanged = emptyList();
    List<Item> beforeKeyChanged = emptyList();
    List<Item> afterKeyChanged = emptyList();

    List<Item> before() {
        List<Item> items = new ArrayList<>();
        items.addAll(unchanged);
        items.addAll(beforeValueChanged);
        items.addAll(beforeKeyChanged);
        items.addAll(removed);
        shuffle(items, Generators.random);
        return unmodifiableList(items);
    }

    List<Item> after() {
        List<Item> items = new ArrayList<>();
        items.addAll(unchanged);
        items.addAll(afterValueChanged);
        items.addAll(afterKeyChanged);
        items.addAll(added);
        shuffle(items, Generators.random);
        return unmodifiableList(items);
    }
}
