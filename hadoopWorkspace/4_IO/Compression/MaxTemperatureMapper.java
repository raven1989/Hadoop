import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureMapper extends MapReduceBase 
       implements Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text value, 
			OutputCollector<Text, IntWritable> output, Reporter reporter) 
	         throws IOException {
		String line = value.toString();
		String year = line.substring(0, 4);
		int airTemperature;
		if(line.charAt(5) == '+'){
			airTemperature = Integer.parseInt(line.substring(6, 10));
		}else{
			airTemperature = Integer.parseInt(line.substring(5, 10));
		}
		output.collect(new Text(year), new IntWritable(airTemperature));
	}

}
