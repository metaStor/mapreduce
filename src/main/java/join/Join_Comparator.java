package join;

/*
 * Created by meta on 19-1-6 下午11:49
 *
 */

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Join_Comparator extends WritableComparator {

    protected Join_Comparator() {
        super(TextPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
//        return super.compare(problem3, problem4);
        TextPair textPair1 = (TextPair) a;
        TextPair textPair2 = (TextPair) b;
        return textPair1.getFirst().compareTo(textPair2.getFirst());
    }
}
