import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;


public class equijoin {

	public static class mapperClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
			String addComma = ",";
			String[] tokens = value.toString().split(addComma);
			Text myKey = new Text(tokens[1]);
			context.write(myKey, value);
		}
	}

	public static class reducerClass extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			Iterator<Text> iterate = values.iterator();
			String table1 = null; 
			String table2 = null;
			ArrayList<String> t1 = new ArrayList<String>();
			ArrayList<String> t2 = new ArrayList<String>();
			String addComma = ",";
			
			while(iterate.hasNext()){
				String tempRow = iterate.next().toString();
				String tempArray[] = tempRow.split(addComma);
				String tablename = tempArray[0];	
				if(table1==null)
					table1 = tablename;
				else if(table2==null){
					if(table1!=tablename){
						table2 = tablename;
					}
				}	
				if(tablename.equals(table1)){
					t1.add(tempRow);
				}
				else if(tablename.equals(table2)){
					t2.add(tempRow);
				}
			}
			for(int i=0; i<t1.size(); i++){
				for(int j=0;j<t2.size();j++){
					String finalResult = t1.get(i) + addComma + " " + t2.get(j);
					context.write(null, new Text(finalResult));	        		
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "My code for equijoin");	
		job.setJarByClass(equijoin.class);	
		job.setMapperClass(mapperClass.class);
		job.setReducerClass(reducerClass.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}