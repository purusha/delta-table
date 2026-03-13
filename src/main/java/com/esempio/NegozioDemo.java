package com.esempio;

import java.util.Arrays;

public class NegozioDemo {

    public static void main(String[] args) {
        System.out.println("\nargs = " + Arrays.toString(args));

        for (String flow : args) {
            if ("01".equals(flow) || "1".equals(flow)) {
                Flow01.start();
            }
        }
    }
}