package mapreduce_test.problem2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Created by ud02 on 1/9/19.
 */
public class ProductText implements Writable {

    private Text Id;
    private Text Name;
    private FloatWritable Price;
    private Text Describute;

    // need it, use for inflect
    public ProductText() {
        set(new Text(), new Text(), new FloatWritable(), new Text());
    }

    public ProductText(Text id, Text name, FloatWritable price, Text describute) {
        set(id, name, price, describute);
    }

    public ProductText(String id, String name, float price, String describute) {
        set(new Text(id), new Text(name), new FloatWritable(price), new Text(describute));
    }

    private void set(Text id, Text name, FloatWritable price, Text describute) {
        this.Id = id;
        this.Name = name;
        this.Price = price;
        this.Describute = describute;
    }

    public Text getName() {
        return Name;
    }

    public void setName(Text name) {
        Name = name;
    }

    public FloatWritable getPrice() {
        return Price;
    }

    public void setPrice(FloatWritable price) {
        Price = price;
    }

    public Text getDescribute() {
        return Describute;
    }

    public void setDescribute(Text describute) {
        Describute = describute;
    }

    public Text getId() {
        return Id;
    }

    public void setId(Text id) {
        Id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProductText)) return false;
        ProductText that = (ProductText) o;
        return Objects.equals(getId(), that.getId()) &&
                Objects.equals(getName(), that.getName()) &&
                Objects.equals(getPrice(), that.getPrice()) &&
                Objects.equals(getDescribute(), that.getDescribute());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getId(), getName(), getPrice(), getDescribute());
    }

    @Override
    public String toString() {
        return "ProductText{" +
                "Id=" + Id +
                ", Name=" + Name +
                ", Price=" + Price +
                ", Describute=" + Describute +
                '}';
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.Id.write(dataOutput);
        this.Name.write(dataOutput);
        this.Price.write(dataOutput);
        this.Describute.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.Id.readFields(dataInput);
        this.Name.readFields(dataInput);
        this.Price.readFields(dataInput);
        this.Describute.readFields(dataInput);
    }

}
