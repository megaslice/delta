package uk.megaslice.delta;

import lombok.EqualsAndHashCode;
import lombok.ToString;

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
}