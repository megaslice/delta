package uk.megaslice.delta;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@ToString
@EqualsAndHashCode
class Item {
    final long id;
    final String key;
    final int value;

    Item(long id, String key, int value) {
        this.id = id;
        this.key = key;
        this.value = value;
    }

    Item withKey(String newKey) {
        return new Item(this.id, newKey, this.value);
    }

    Item withValue(int newValue) {
        return new Item(this.id, this.key, newValue);
    }

    static final NaturalKey<Item, String> naturalKey = item -> item.key;

    static Map<String, Item> toMap(Collection<Item> items) {
        Map<String, Item> itemMap = new HashMap<>();
        for (Item item : items) {
            itemMap.put(naturalKey.getNaturalKey(item), item);
        }
        return itemMap;
    }
}