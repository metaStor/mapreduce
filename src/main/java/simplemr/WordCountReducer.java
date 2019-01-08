package simplemr;

import simplemr.Pair;
import simplemr.ReduceTask;

import java.util.List;

public class WordCountReducer implements ReduceTask {

    @Override
    public Pair reduce(String key, List<String> valueList) {
        int sum = 0;
        for (String v : valueList) {
            int count = Integer.parseInt(v);
            sum += count;
        }
        return new Pair(key, Integer.toString(sum));
    }

}
