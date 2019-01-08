package simplemr;

import simplemr.MapTask;
import simplemr.Pair;

import java.util.ArrayList;
import java.util.List;

public class WordCountMapper implements MapTask {

    @Override
    public List<Pair> map(String key, String value) {
        ArrayList<Pair> out = new ArrayList<Pair>();
        String[] words = value.split("[^a-zA-Z0-9]+");
        for (String word : words) {
            if (!word.isEmpty()) {
                out.add(new Pair(word, "1"));
            }
        }
        return out;
    }

}
