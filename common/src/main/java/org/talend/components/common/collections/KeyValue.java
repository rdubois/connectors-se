/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.common.collections;

import java.util.Map;
import java.util.function.Function;

/**
 * Allow easier stream function usage with map
 * originMap.entrySet() //
 * .stream() //
 * .map(KeyVal::fromEntry) // entrySet to key val
 * .map(KeyVal.keyFunc(this::changeKey)) // apply changeKey to key of KeyVal instances
 */
public class KeyValue<K, V> {

    private final K key;

    private final V value;

    public KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public static <K, V> KeyValue<K, V> fromEntry(Map.Entry<K, V> e) {
        return new KeyValue<>(e.getKey(), e.getValue());
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    /**
     * Apply a function on key to get a new KeyVal instance.
     * 
     * @param fkey : function to apply on key.
     * @return new KeyVal.
     */
    public <K1> KeyValue<K1, V> applyKey(Function<K, K1> fkey) {
        final K1 newKey = fkey.apply(this.key);
        return new KeyValue<>(newKey, this.value);
    }

    /**
     * Transform key transform function to KeyVal transform function.
     * (Simplify usage in stream)
     * 
     * @param fkey : key function.
     * @return KeyValue function.
     */
    public static <K, K1, V> Function<KeyValue<K, V>, KeyValue<K1, V>> keyFunc(Function<K, K1> fkey) {
        return (KeyValue<K, V> kv) -> kv.applyKey(fkey);
    }

    /**
     * Apply a function on key to get a new KeyVal instance.
     * 
     * @param fValue : function to apply on value.
     * @return new KeyValue.
     */
    public <V1> KeyValue<K, V1> applyValue(Function<V, V1> fValue) {
        final V1 newValue = fValue.apply(this.value);
        return new KeyValue<>(this.key, newValue);
    }

    /**
     * Transform key transform function to KeyVal transform function.
     * (Simplify usage in stream)
     * 
     * @param fValue : key function.
     * @return keyVal function.
     */
    public static <K, V, V1> Function<KeyValue<K, V>, KeyValue<K, V1>> valueFunc(Function<V, V1> fValue) {
        return (KeyValue<K, V> kv) -> kv.applyValue(fValue);
    }
}
