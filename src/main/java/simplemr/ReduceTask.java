package simplemr;

import java.util.List;

public interface ReduceTask {

    Pair reduce(String key, List<String> valueList);

}
