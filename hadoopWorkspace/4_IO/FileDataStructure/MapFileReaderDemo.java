import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileReaderDemo {

  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    String uri = args[0];
    Configuration conf = new Configuration();
    Path path = new Path(uri);
    MapFile.Reader reader = null;
    MapFile.Reader readerInterval = null;
    try {
      reader = new MapFile.Reader(path, conf);
      WritableComparable key = ReflectionUtils.newInstance(
          reader.getKeyClass().asSubclass(WritableComparable.class), 
          conf);
      Writable value = (Writable)
          ReflectionUtils.newInstance(reader.getValueClass(), conf);
      while(reader.next(key, value)){ 
        System.out.printf("%s\t%s\n", key, value);
      }
      
      //同样也支持随机读
      System.out.println("-------------------------------------------------------Random-------------------------------------------------------");
      key = new IntWritable(500);
      reader.get(key, value);
      System.out.printf("%s\t%s\n", key, value);
      
      //MapFile的Writer可以设置索引间隔setIndexInterval()，影响了index文件的大小
      //这个索引是Reader要读入内存的，已经写定大小的index文件可能对Reader来讲太大了
      //因此，Reader可以设置读入内存的index大小，通过设置io.map.index.skip，表示跳过几个索引读取一个，默认值0
      conf.setInt("io.map.index.skip", 1);
      readerInterval = new MapFile.Reader(path, conf);
      key = new IntWritable(499);
      readerInterval.get(key, value);
      System.out.printf("%s\t%s\n", key, value);
    } finally {
      IOUtils.closeStream(reader);
      IOUtils.closeStream(readerInterval);
    }

  }

}
