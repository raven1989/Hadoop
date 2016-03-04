import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

  //mapper
  static class SequenceFileMapper extends MapReduceBase
    implements Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    private JobConf conf;
    
    @Override
    public void configure(JobConf conf) {
      this.conf = conf;
    }
    public void map(NullWritable key, BytesWritable value, 
        OutputCollector<Text, BytesWritable> output, Reporter reporter) 
            throws IOException {
      String filename = conf.get("map.input.file");
      output.collect(new Text(filename), value);
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
    conf.setJobName("Small files 2 SequenceFile");
    conf.setJar("s2sf.jar");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setInputFormat(WholeFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(BytesWritable.class);
    
    conf.setMapperClass(SequenceFileMapper.class);
    conf.setReducerClass(IdentityReducer.class); //这个是默认的Reducer
    
    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
    System.exit(exitCode);

  }

}
