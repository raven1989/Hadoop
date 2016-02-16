import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class WritableImplements {

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    BytesWritable b = new BytesWritable(new byte[]{3,5});
    byte[] bytes = WritableBasics.serialize(b);
    //BytesWritable序列化为4字节长度跟字节本身
    System.out.println("BytesWritable: 0x"+StringUtils.byteToHexString(bytes));
    
    b.setCapacity(11);
    //bytes length不表示真实有效的数据长度，length才是
    System.out.println("BytesWritable: length:"+b.getLength()+" bytes length:"+b.getBytes().length);
    
    NullWritable n = NullWritable.get();
    System.out.println("NullWritable: "+n.toString());
    
    ArrayWritable a = new ArrayWritable(Text.class);
    a.set(new Text[]{new Text("hello"), new Text("hadoop")});
    ArrayList<String> res = new ArrayList<String>();
    for(Writable it : a.get()){
      res.add(it.toString());
    }
    System.out.println("ArrayWritable: "+res);
    
    MapWritable m1 = new MapWritable();
    m1.put(new IntWritable(1), new Text("cat"));
    m1.put(new VIntWritable(2), new LongWritable(163));
    MapWritable m2 = new MapWritable();
    m2.putAll(m1);
    System.out.println("MapWritable: "+m2.get(new IntWritable(1))+" "+m2.get(new VIntWritable(2))); 

  }

}
