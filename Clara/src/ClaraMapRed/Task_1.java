package ClaraMapRed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task_1 {
	public static void main(String [] args) throws Exception
    {
	    Configuration c=new Configuration();
	    String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
	    Path input=new Path(files[0]);
	    Path output=new Path(files[1]);
	    Job j=new Job(c,"ClaraMapReduce");
	    j.setJarByClass(Task_1.class);
	    j.setMapperClass(MapForTask_1.class);
	    j.setReducerClass(ReduceForTask_1.class);
	    j.setMapOutputKeyClass(LongWritable.class);
	    j.setMapOutputValueClass(DoubleArrayWritable.class);
	    j.setOutputKeyClass(LongWritable.class);
	    j.setOutputValueClass(DoubleArrayWritable.class);
	    FileInputFormat.addInputPath(j, input);
	    FileOutputFormat.setOutputPath(j, output);
	    System.exit(j.waitForCompletion(true)?0:1);
    }
	 
    public static class MapForTask_1 extends Mapper<LongWritable, Text, LongWritable, DoubleArrayWritable>{
    	
    	Random random = new Random();
    	
    	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		    
    		String line = value.toString();
		    double row[] = Arrays.stream(line.split(",")).mapToDouble(Double::parseDouble).toArray();
		    LongWritable outputKey = new LongWritable(random.nextLong());
	    	DoubleArrayWritable outputValue = new DoubleArrayWritable(row);
	    	con.write(outputKey, outputValue);
    	}

    }
    public static class ReduceForTask_1 extends Reducer<LongWritable, DoubleArrayWritable, LongWritable, DoubleArrayWritable>{
    	
    	public void run(Context con) throws IOException, InterruptedException {
    		
    		setup(con);
    		int rows = 0;
    		while(con.nextKeyValue()) {
    			
    			if(rows++ == 1450)
    				break;
    			reduce(con.getCurrentKey(), con.getCurrentValue(), con);
    		}
    		cleanup(con);
    	}
    	
	    public void reduce(LongWritable key, DoubleArrayWritable value, Context con) throws IOException, InterruptedException{
	    	
	    	con.write(key, value);
	    }
    }
    
    static class DoubleArrayWritable implements Writable {
        private double[] data;

        public DoubleArrayWritable() {

        }

        public DoubleArrayWritable(double[] data) {
            this.data = data;
        }

        public double[] getData() {
            return data;
        }

        public void setData(double[] data) {
            this.data = data;
        }

        public void write(DataOutput out) throws IOException {
            int length = 0;
            if(data != null) {
                length = data.length;
            }

            out.writeInt(length);

            for(int i = 0; i < length; i++) {
                out.writeDouble(data[i]);
            }
        }

        public void readFields(DataInput in) throws IOException {
            int length = in.readInt();

            data = new double[length];

            for(int i = 0; i < length; i++) {
                data[i] = in.readDouble();
            }
        }
    }
}
