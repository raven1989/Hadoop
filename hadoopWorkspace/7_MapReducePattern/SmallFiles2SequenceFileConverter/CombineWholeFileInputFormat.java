import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

public class CombineWholeFileInputFormat 
  extends CombineFileInputFormat<NullWritable, BytesWritable> {

  private static class CombineWholeFileRecordReaderWrapper 
    extends CombineFileRecordReaderWrapper<NullWritable, BytesWritable> {

    public CombineWholeFileRecordReaderWrapper(CombineFileSplit split, 
        Configuration conf, Reporter reporter, Integer idx) throws IOException {
      // TODO Auto-generated constructor stub
      //这里需要继承了FileInputFormat<K,V>的，而不是CombineFileInputFormat<K,V>的类实例
      super( new WholeFileInputFormat(), split, conf, reporter, idx);
    }
    
  }
  
  @Override
  protected boolean isSplitable(FileSystem fs, Path filename){
    return false;
  }
  @Override
  public RecordReader<NullWritable, BytesWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) 
          throws IOException {
    // TODO Auto-generated method stub
    return new CombineFileRecordReader(job, (CombineFileSplit)split, reporter, 
        CombineWholeFileRecordReaderWrapper.class);
  }

}
