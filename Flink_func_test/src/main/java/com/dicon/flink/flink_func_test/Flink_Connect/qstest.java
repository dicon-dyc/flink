package com.dicon.flink.flink_func_test.Flink_Connect;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * 快速排序
 * @Author: dyc
 * @Create: 2023/7/19 1:07
 */

public class qstest {

    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String input;

        while ((input = br.readLine())!=null){

            String[] split = input.split(",");

            int[] ints = Arrays.asList(split).stream().mapToInt(Integer::parseInt).toArray();

            quicSort(ints,0,ints.length-1);

            System.out.println(Arrays.toString(ints));
        }
    }

    /**
     * 交换数组中的两个元素
     * @param arr 数组
     * @param left 第一个元素
     * @param right 第二个元素
     */
    public static void swap(int[] arr,int left,int right){

        int tmp = arr[left];

        arr[left] = arr[right];

        arr[right] = tmp;
    }

    public static void quicSort(int[] arr,int left, int right){

        if (left >= right){
            return;
        }

        int i = left;
        int j = right;

        int pivot = arr[left];

        while (i < j){
            while (j > i && arr[j] >= pivot){
                j--;
            }

            while (i < j && arr[i] <= pivot){
                i++;
            }

            swap(arr,i,j);
        }
        arr[left] = arr[i];
        arr[i] = pivot;
        quicSort(arr,left,i-1);
        quicSort(arr,i+1,right);

    }

}
