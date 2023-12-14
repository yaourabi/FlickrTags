package org.example;

import org.apache.hadoop.io.WritableComparator;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class StringAndIntComparator extends WritableComparator {

    public StringAndIntComparator() {
        super(StringAndInt.class, true);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        StringAndInt key1 = new StringAndInt();
        StringAndInt key2 = new StringAndInt();

        try {
            key1.readFields(new DataInputStream(new ByteArrayInputStream(b1, s1, l1)));
            key2.readFields(new DataInputStream(new ByteArrayInputStream(b2, s2, l2)));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        // Implement the comparison logic
        return key1.compareTo(key2);
    }
}
