package com.chris.dataming.alg;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

import org.mortbay.log.Log;

public class Combination {
	private int DIKAER = 2;//item * item 
    private ArrayList<HashSet<Integer>> numberSet; 
    private HashSet<HashSet<String>> result; 
    
    /**
     * c (n k), return set
     * */
	public  ArrayList<HashSet<String>>getSetFrom(ArrayList<HashSet<String>> list, int k){
		
		numberSet = new ArrayList<HashSet<Integer>>();
		result = new HashSet<HashSet<String>>();
	
		
		int length = list.size();
		if (1 == length){
			return null;
		}
		int a[] = new int[length];
		Log.info("ArrayList<HashSet<String>>list.size:"+list.size());
		initArray(a, length);
		
		combine(a, DIKAER);
		
        for(HashSet<Integer> l: numberSet){
            HashSet<String> tmp = new HashSet<String>();
            for (Integer i:l){
                tmp.addAll(list.get(i));
            }
            //tmp的长度是k项 且tmp的k-1子项也是频繁的
            if(tmp.size()==k && isInList(tmp,k-1,list)){
            	result.add(tmp);
            	
            }
        }
        

		//return result ;
		return new ArrayList<HashSet<String>>(result);
	}
    ////剪枝
	private boolean isInList(HashSet<String> s,int n,
			ArrayList<HashSet<String>> list) {
		// TODO Auto-generated method stub
		System.out.println(s.size()+"-1===="+n);
		boolean flag = false;
		LinkedList<String> tmp = new LinkedList<String>();
		
		for(int i=0;i<s.size();i++){
			tmp.clear();
			tmp.addAll(s);
			tmp.remove(i);
			
			for(int j=0;j<list.size();j++){
				if(list.get(j).containsAll(tmp)){
					flag = true;
					break;
				}
			}
			if(flag){
				flag = false;
			}else{
				return false;
			}
		}
		return true;
	}

	private int initArray(int a[], int n){
        for (int i = 0; i < n; i++) {
            a[i] = i;
        }
		return 0;
	}
	
    public void combine(int[] a, int n) {

        if(null == a || a.length == 0 || n <= 0 || n > a.length)
            return ;

        int[] b = new int[n];//辅助空间，保存待输出组合数
        getCombination(a, n , 0, b, 0);
    }

    private void getCombination(int[] a, int n, int begin, int[] b, int index) {

        if(n == 0){//如果够n个数了，输出b数组
        	HashSet<Integer> bList = new HashSet<Integer>();
            for(int i = 0; i < index; i++){
                bList.add(b[i]);
            }
            numberSet.add(bList);
            return;
        }

        for(int i = begin; i < a.length; i++){

            b[index] = a[i];
            getCombination(a, n-1, i+1, b, index+1);
        }

    }
    
    
    
    //result is the c(n,k),but we should be more spefic,
    //cause we still need to delete the not frequency ones  
    public  void deleteUnFrequency(ArrayList<HashSet<String>> newList,
    		ArrayList<HashSet<String>> oldList, int k){
    	//ArrayList<HashSet<String>> newListKminus1Set = this.getSetFrom(newList, k-1);
    	
    	//return ;
    }
}
