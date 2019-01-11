
/*
 * Create By meta On 19-1-8 下午3:29
 */

import mapreduce_test.problem2.ProductText;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
}
