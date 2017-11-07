package TASK2;


import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import PAM.Point;
import TASK2.CandidateSetofMedoids;
public class Task2 {
	public static Map<Integer,ArrayList<Integer>> candidateset;
	public static ArrayList<Point> data;
	public static void main(String [] args) throws Exception
    {
	    Configuration c=new Configuration();
	    String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
	    Path input=new Path(files[0]);
	    Path output=new Path(files[1]);
	    CandidateSetofMedoids cd=new CandidateSetofMedoids();
	    candidateset=cd.getcandidateset();
	    data = cd.getdata();
	    Job j=new Job(c,"ClaraMapReduce");
	    j.setJarByClass(Task2.class);
	    j.setMapperClass(MapForTask_2.class);
	    j.setReducerClass(ReduceForTask_1.class);
	    j.setMapOutputKeyClass(LongWritable.class);
	    j.setMapOutputValueClass(DoubleWritable.class);
	    j.setOutputKeyClass(LongWritable.class);
	    j.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(j, input);
	    FileOutputFormat.setOutputPath(j, output);
	    System.exit(j.waitForCompletion(true)?0:1);
    }
	 
    public static class MapForTask_2 extends Mapper<LongWritable, Text, LongWritable, DoubleWritable>{
    	
    	Random random = new Random();
    	
    	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		    
    		String line = value.toString();
		    double row[] = Arrays.stream(line.split(",")).mapToDouble(Double::parseDouble).toArray();
		    Point p=new Point(row[0],row[1]);
		    
			Iterator<Integer> iterator = candidateset.keySet().iterator();
			
		    while (iterator.hasNext()) {
		    		Double distance=0.0;
		            int newkey = Integer.parseInt(iterator.next().toString());
		            LongWritable outputKey = new LongWritable(newkey);
		            ArrayList<Integer> newvalue = candidateset.get(newkey);
		            distance= p.distance(p, data.get(newvalue.get(0)));
		            for(int a:newvalue)
		            {
		            	Double newdistance=p.distance(p, data.get(a));
		            	if(newdistance<distance);
		            		distance=newdistance;
		            }
		            DoubleWritable outputValue = new DoubleWritable(distance);
			    	//DoubleArrayWritable outputValue = new DoubleArrayWritable(distance);
			    	con.write(outputKey, outputValue);           
		}
    	}

    }
    public static class ReduceForTask_1 extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable>{
    	
	    public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException{
	    	double sum = 0.0;

	    	   for(DoubleWritable value : values)

	    	   {

	    	   sum += value.get();

	    	   }

	    	   con.write(key, new DoubleWritable(sum));
	    }
    }
}    
   /* static class DoubleArrayWritable implements Writable {
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
*/