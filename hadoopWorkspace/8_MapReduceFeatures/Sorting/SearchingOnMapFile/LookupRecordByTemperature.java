import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LookupRecordByTemperature extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    // TODO Auto-generated method stub
    if(args.length != 2){
      System.out.printf("Usage: %s [generic options] <path> <key>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    
    Path path = new Path(args[0]);
    IntWritable key = new IntWritable(Integer.parseInt(args[1]));
    FileSystem fs = path.getFileSystem(getConf());
    
    //getReaders打开路径下的所有map文件，每个文件一个reader
    Reader[] readers = MapFileOutputFormat.getReaders(fs, path, getConf());
    Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
    
    Text val = new Text();
    //getEntry用partitioner去locate参数key落到的reader，再调用reader的get方法获取val
    Writable entry = MapFileOutputFormat.getEntry(readers, partitioner, key, val);
    if(entry == null) {
      System.err.println("Key not found: "+key);
      return -1;
    }
    
    System.out.println(val.toString());
    
    return 0;
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int exitCode = ToolRunner.run(new LookupRecordByTemperature(), args);
    System.exit(exitCode);

  }

}
