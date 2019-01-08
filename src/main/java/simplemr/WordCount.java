package simplemr;

import simplemr.SimpleMapReduce;

public class WordCount {

    public static void main(String[] args) {
        String inPath = args[0];
        String outPath = args[1];
        WordCountMapper mapper = new WordCountMapper();
        WordCountReducer reducer = new WordCountReducer();
        SimpleMapReduce.run(inPath, outPath, mapper, reducer);
    }

}
