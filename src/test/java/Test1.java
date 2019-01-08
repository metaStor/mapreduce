
/*
 * Create By meta On 19-1-8 下午3:29
 */

import org.junit.Test;

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
}
