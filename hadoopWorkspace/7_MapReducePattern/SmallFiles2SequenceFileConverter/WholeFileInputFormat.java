import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

//把整个文件作为一条记录
public class WholeFileInputFormat 
  extends FileInputFormat<NullWritable, BytesWritable> {
  
  //指定文件不被分片
  @Override
  protected boolean isSplitable(FileSystem fs, Path filename){
    return false;
  }
  
  @Override
  public RecordReader<NullWritable, BytesWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new WholeFileRecordReader((FileSplit)split, (Configuration)job);
  }

}
