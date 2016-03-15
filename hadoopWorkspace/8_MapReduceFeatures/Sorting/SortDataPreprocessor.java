import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortDataPreprocessor extends Configured implements Tool {
  //这里没有排序，只是剔除了错误记录

  static class CleanerMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, IntWritable, Text> {
    private ClimateRecordParser parser = new ClimateRecordParser();
    
    public void map(LongWritable key, Text value, 
        OutputCollector<IntWritable, Text> output, Reporter reporter) 
            throws IOException {
      parser.parse(value);
      if(parser.isValidTemperature()){
        output.collect(new IntWritable(parser.getAirTemperature()), value);
      }
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
    conf.setJobName("Sort Data by Mapper");
    conf.setJar("sdp.jar");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setMapperClass(CleanerMapper.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setNumReduceTasks(0);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
    
    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
    System.exit(exitCode);

  }

}
