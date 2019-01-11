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
    private String location;

    public Department() {
    }

    public Department(String id, String dept, String location) {
        this.id = id;
        this.dept = dept;
        this.location = location;
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

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "Department{" +
                "id='" + id + '\'' +
                ", dept='" + dept + '\'' +
                ", location='" + location + '\'' +
                '}';
    }

    // 将对象序列化到数据流
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(dept);
        dataOutput.writeUTF(location);
    }

    //从数据流中反序列出对象的数据
    //从数据流中读取字段时必须和序列化的顺序保持一致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.dept = dataInput.readUTF();
        this.location = dataInput.readUTF();
    }
}
