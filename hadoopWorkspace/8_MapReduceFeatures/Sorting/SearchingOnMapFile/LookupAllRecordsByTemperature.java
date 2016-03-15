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

public class LookupAllRecordsByTemperature extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    // TODO Auto-generated method stub
    if(args.length != 2){
      System.out.printf("Usage: %s [generic options] <path> <key>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    Path path = new Path(args[0]);
    IntWritable key = new IntWritable(Integer.parseInt(args[1]) );
    FileSystem fs = path.getFileSystem(getConf());
    
    Reader[] readers = MapFileOutputFormat.getReaders(fs, path, getConf());
    Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
    Text val = new Text();
    
    //readers按照分区排序，getPartition获取key对应的partition number
    Reader reader = readers[partitioner.getPartition(key, val, readers.length)];
    //获取第一条记录
    Writable entry = reader.get(key, val);
    if(entry == null) {
      System.err.println("Key not found: "+key);
      return -1;
    }
    //这样获取余下的记录：
    //因为所有相同的key都在一个partitioner中，用next获取下一条记录，直到key变化
    IntWritable nextKey = new IntWritable();
    do{
      System.out.println(val.toString());
    } while(reader.next(nextKey, val) && key.equals(nextKey));
    
    return 0;
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int exitCode = ToolRunner.run(new LookupAllRecordsByTemperature(), args);
    System.exit(exitCode);

  }

}
