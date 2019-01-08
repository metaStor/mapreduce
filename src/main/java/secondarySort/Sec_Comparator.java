package secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * Create By meta On 19-1-7 下午8:40
 */
public class Sec_Comparator extends WritableComparator {

    protected Sec_Comparator() {
        super(IntPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntPair obj1 = (IntPair) a;
        IntPair obj2 = (IntPair) b;
        // 先按第一字段升序
        int cmp = obj1.getFirst().compareTo(obj2.getFirst());
        if (cmp != 0) {
            return cmp;
        }
        // 第一字段相等的情况下，按照第二字段降序
        return -(obj1.getSecond().compareTo(obj2.getSecond()));
    }
}
