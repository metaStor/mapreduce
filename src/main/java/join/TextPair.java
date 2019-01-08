package join;

/*
 * Created by meta on 19-1-6 下午10:40
 *
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {

    private Text first;  // values
    private Text second;  // label

    // 添加无参的构造方法,否则反射时会报错
    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    // 方法重载
    public TextPair(Text first, Text second) {
        set(first, second);
    }

    public Text getFirst() {
        return first;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public Text getSecond() {
        return second;
    }

    public void setSecond(Text second) {
        this.second = second;
    }

    private void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(TextPair o) {
        int comp = this.first.compareTo(o.first);
        if (comp != 0) {
            return comp;
        }
        return this.second.compareTo(o.second);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }
}
