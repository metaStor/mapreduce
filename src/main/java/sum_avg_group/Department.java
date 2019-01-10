package sum_avg_group;

/*
 * Created by meta on 19-1-10 下午9:41
 *
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Department implements Writable {

    private String id;
    private String dept;

    public Department() {
    }

    public Department(String id, String dept) {
        this.id = id;
        this.dept = dept;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDept() {
        return dept;
    }

    public void setDept(String dept) {
        this.dept = dept;
    }

    @Override
    public String toString() {
        return "Department{" +
                "id='" + id + '\'' +
                ", dept='" + dept +
                '}';
    }

    // 将对象序列化到数据流
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(dept);
    }

    //从数据流中反序列出对象的数据
    //从数据流中读取字段时必须和序列化的顺序保持一致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        dept = dataInput.readUTF();
    }
}
