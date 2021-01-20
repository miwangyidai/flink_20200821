package day02;

import java.util.ArrayList;
import java.util.Iterator;

public class test01 {
    public static void main(String[] args) {
        System.out.println("begin");
        sss();
        System.out.println("end");
    }
    public static void sss(){
        ArrayList<String> ssss = new ArrayList<String>();
        ssss.add("huhan");
        ssss.add("huhan11");
        ssss.add("huhan2");
        ssss.add("huhan3");

        Iterator<String> iterator = ssss.iterator();
        while (iterator.hasNext()){
            String next = iterator.next();
            if (next.equals("huhan11")){
                ssss.add("假货");
            }
        }
    }
}
