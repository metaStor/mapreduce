
/*
 * Create By meta On 19-1-8 下午3:29
 */

import mapreduce_test.problem2.ProductText;
import org.junit.Test;

import java.util.*;

public class Test1 {

    @Test
    public void test() {
        String words = "hello everyone!you, her, me. ok?";
        StringTokenizer tokenizer = new StringTokenizer(words, " ,.?!\r\n\t\f", false);
        System.out.println(String.format("There are %s words in this sentance", tokenizer.countTokens()));
        while (tokenizer.hasMoreTokens()) {
            System.out.println(tokenizer.nextToken());
        }
    }

    @Test
    public void objectTest() {
        List<ProductText> products = new ArrayList<>();
        ProductText a1;
        a1 = new ProductText("123", "None", 0, "");
        products.add(a1);
        a1 = new ProductText("122", "None", 0, "");
        products.add(a1);
        for (ProductText product : products) {
            System.out.println(System.identityHashCode(product));
        }
    }

    @Test
    public void stringComparator() {
        System.out.println("18".compareTo("19"));
    }

    @Test
    public void splitTest() {
        String str = "1,2,3,,4";
        String[] sp = str.split(",");
        System.out.println(sp[3].equals(""));
    }

    @Test
    public void MapTest() {
        Map<String, String> map = new HashMap<>();
        map.put("1", "2");
        map.put("2", "3");
        map.put("3", "5");
        System.out.println(map.get("3"));
//        for (Map.Entry<String, String> entry : map.entrySet()) {
//            System.out.println(entry.getKey() + "\t" + entry.getValue());
//        }
    }
}
