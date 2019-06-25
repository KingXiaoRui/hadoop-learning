package com.king.demo.test;

import java.util.Arrays;

/**
 * @author DKing
 * @description
 * @date 2019/6/12
 */
public class Test {
    public static void main(String[] args) {
        String test = "[1]+[2]";
        test = test.replaceAll("\\[", "");
        System.out.println(test);

        String[] strings = test.split("\\]");
        System.out.println(strings.length);
        System.out.println(strings[0]);
        System.out.println(strings[1]);
    }
}
