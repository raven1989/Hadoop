import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureWithCounters extends Configured implements Tool {
  enum Temperature {
    MISSING,
    MALFORMED
  }
  
  static class MaxTemperatureMapperWithCounters extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, IntWritable> {
    private ClimateRecordParser parser = new ClimateRecordParser();
    
    public void map(LongWritable key, Text value, 
        OutputCollector<Text, IntWritable> output, Reporter reporter) 
            throws IOException {
      parser.parse(value);
      if(parser.isValidTemperature()){
        int airTemperature = parser.getAirTemperature();
        output.collect(new Text(parser.getYear()), new IntWritable(airTemperature));
      }else if(parser.isMalformedTemperature()){
        System.err.println("Ignoring possible corrupt input: "+value);
        reporter.incrCounter(Temperature.MALFORMED, 1);
      }else if(parser.isMissingTemperature()){
        System.err.println("Ignoring possible missing tmperature: "+value);
        reporter.incrCounter(Temperature.MISSING, 1);
      }
      //dynamic counter
      if(!parser.isMalformedTemperature())
        reporter.incrCounter("StationId", parser.getStationId(), 1);
    }
  }

  public int run(String[] args) throws Exception {
    // TODO Auto-generated method stub
    if(args.length != 2){
      System.out.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("MaxTemperature With Counters");
    conf.setJar("mtc.jar");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(MaxTemperatureMapperWithCounters.class);
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducer.class);
    
    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
    System.exit(exitCode);

  }

}
