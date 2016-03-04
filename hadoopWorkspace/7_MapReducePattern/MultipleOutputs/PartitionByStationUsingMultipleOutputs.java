import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//将整个数据集切分为以气象站ID命名的文件
public class PartitionByStationUsingMultipleOutputs extends Configured implements Tool {

  static class StationMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, Text> {
    private ClimateRecordParser parser = new ClimateRecordParser();
    public void map(LongWritable key, Text value, 
        OutputCollector<Text, Text> output, Reporter reporter) 
            throws IOException {
      parser.parse(value);
      output.collect(new Text(parser.getStationId()), value);
    }
  }
  
  static class MultipleOutputsReducer extends MapReduceBase 
    implements Reducer<Text, Text, NullWritable, Text> {
    private MultipleOutputs multipleOutputs;
    
    @Override
    public void configure(JobConf conf) {
      multipleOutputs = new MultipleOutputs(conf);
    }
    
    public void reduce(Text key, Iterator<Text> values, 
        OutputCollector<NullWritable, Text> output, Reporter reporter) 
            throws IOException {
      //这里为每一个key获取一个collector，并用station和key生成唯一命名
      OutputCollector collector = multipleOutputs.getCollector("station", key.toString(), reporter);
      while(values.hasNext()) {
        collector.collect(NullWritable.get(), values.next());
      }
    }
    
    @Override
    public void close() throws IOException {
      multipleOutputs.close();
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
    conf.setJobName("Partition by station using MultipleOutputs");
    conf.setJar("pbs.jar");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setMapperClass(StationMapper.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setReducerClass(MultipleOutputsReducer.class);
    //不让原来的output去写输出，在reduce函数中也看出来了，没有使用参数中的output
    conf.setOutputFormat(NullOutputFormat.class);
    MultipleOutputs.addMultiNamedOutput(conf, "station", 
        TextOutputFormat.class, NullWritable.class, Text.class);
    
    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(), args);
    System.exit(exitCode);

  }

}
