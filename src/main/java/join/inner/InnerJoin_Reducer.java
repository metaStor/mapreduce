package join.inner;

/*
 * Created by meta on 19-1-6 下午11:16
 *
 */

import join.TextPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InnerJoin_Reducer extends Reducer<TextPair, Text, Text, Text> {

    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        Text keyin = key.getFirst();
        String value = values.iterator().next().toString();
        while (values.iterator().hasNext()) {
            context.write(keyin, new Text(values.iterator().next().toString() + "\t" + value));
        }
    }
}
