import java.io.*;

import org.apache.hadoop.io.Writable;

public class ContributionValue implements Writable {
    public long col;
    public double contribution;

    public ContributionValue(){

    }
    public ContributionValue(long col, double contribution){
        this.col = col;
        this.contribution = contribution;
    }

    public	void write(DataOutput out) throws IOException{
        out.writeLong(col);
        out.writeDouble(contribution);
    }

    public void readFields(DataInput in) throws IOException{
        this.col = in.readLong();
        this.contribution = in.readDouble();
    }

}