package org;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class testEquality {
    public static void main(String[] args) {
        List<Long> rec1= new ArrayList<>();
        List<Long> rec2 = new ArrayList<>();

        rec1.add(1L);
        rec1.add(2L);
        rec1.add(3L);

        rec2.add(1L);
        rec2.add(2L);
        rec2.add(3L);

        boolean areEqual = rec1.equals(rec2);
        if (areEqual) {
            System.out.println("The two records are equal.");
        } else {
            System.out.println("The two records are not equal.");
        }

        HashMap<List<Long>, Integer> map = new HashMap<>();
        map.put(rec1, 1);
        System.out.println("Map size: " + map.size());
        boolean containsRec1 = map.containsKey(rec1);
        boolean containsRec2 = map.containsKey(rec2);
        System.out.println("Map contains rec1: " + containsRec1);
        System.out.println("Map contains rec2: " + containsRec2);

        map.remove(rec2);
        System.out.println("Map size after removing rec2: " + map.size());
        boolean containsRec2AfterRemoval = map.containsKey(rec2);
        boolean containsRec1AfterRemoval = map.containsKey(rec1);
        System.out.println("Map contains rec2 after removal: " + containsRec2AfterRemoval);
        System.out.println("Map contains rec1 after removal: " + containsRec1AfterRemoval);


    }

}
