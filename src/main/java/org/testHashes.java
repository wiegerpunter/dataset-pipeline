package org;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import com.google.common.hash.HashFunction;
public class testHashes {


    public static void main(String[] args) {
        HashFunction[] hashFunctions = new HashFunction[3];
        for (int i = 0; i < 3; i++) {
            hashFunctions[i] = Hashing.murmur3_32_fixed(i + 0);
        }

        long attrValue = 5L;
        String stringValue = "5";
        int depth = 3;
        int width = 28;
        int[] hashes = new int[depth];
        computeHashes(hashFunctions, attrValue, depth, width, hashes);
        System.out.println("Hashes for long value:");
        for (int hash : hashes) {
            System.out.println(hash);
        }
        computeHashes(hashFunctions, stringValue, depth, width, hashes);
        System.out.println("Hashes for string value:");
        for (int hash : hashes) {
            System.out.println(hash);
        }
    }

    public static void computeHashes(HashFunction[] hashFunctions, Object attrValue, int depth, int width, int[] hashes) {

        if (attrValue instanceof String) {
            for (int i = 0; i < depth; i++) {
                int hash = hashFunctions[i].hashString((String) attrValue, StandardCharsets.UTF_8).asInt();
                hashes[i] = fastPositiveModulo(hash, width);
            }
            return;
        }else if (attrValue instanceof Long) {
            long longValue = (Long) attrValue;
            for (int i = 0; i < depth; i++) {
                int hash = hashFunctions[i].hashLong((longValue)).asInt();
                hashes[i] = fastPositiveModulo(hash, width);
            }
            return;
        }
        throw new IllegalArgumentException("Unsupported attribute type: " + attrValue.getClass().getName());
    }



    // Fast modulo for positive results
    private static int fastPositiveModulo(int x, int mod) {
        int r = x & (mod - 1); // works if mod is a power of two
        return mod == (mod & -mod) ? r : (x % mod + mod) % mod;
    }

}
