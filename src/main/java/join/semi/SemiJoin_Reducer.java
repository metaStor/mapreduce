package join.semi;

import join.TextPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * Create By meta On 19-1-8 下午8:25
 */
public class SemiJoin_Reducer extends Reducer<TextPair, Text, Text, Text> {

    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String user = key.getFirst().toString();
        String value = values.iterator().next().toString();
//        System.out.println(value);
        while (values.iterator().hasNext()) {
            context.write(new Text("User: " + user), new Text(value + "\t" +
                    "Logs: " + values.iterator().next().toString()));
        }
    }
}
