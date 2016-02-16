import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class CustomizedWritable {
  
  public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;
    
    public TextPair(){
      set(new Text(), new Text());
    }
    
    public TextPair(String first, String second){
      set(new Text(first), new Text(second));
    }
    
    public TextPair(Text first, Text second){
      set(first, second);
    }
    
    public void set(Text first, Text second){
      this.first = first;
      this.second = second;
    }
    
    public Text getFirst(){
      return first;
    }
    
    public Text getSecond(){
      return second;
    }
    
    public void write(DataOutput out) throws IOException {
      first.write(out);
      second.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
      first.readFields(in);
      second.readFields(in);
    }
    
    public int hashCode(){
      return first.hashCode() * 163 + second.hashCode();
    }
    
    public boolean equals(Object o){
      if(o instanceof TextPair){
        TextPair tp = (TextPair)o;
        return tp.first.equals(first) && tp.second.equals(second);
      }
      return false;
    }
    
    public String toString(){
      return first + "\t" + second;
    }

    public int compareTo(TextPair tp) {
      // TODO Auto-generated method stub
      int cmp = first.compareTo(tp.first);
      if(cmp == 0 ){
        return second.compareTo(tp.second);
      }
      return cmp;
    }
  }
  
  public static class TextPairComparator extends WritableComparator {
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    
    public TextPairComparator() {
      super(Text.class);
    }
    
    //Text序列化后首先是一个变长整数VInt表示随后编码的长度，
    //readVInt读取的是这个值，decodeVIntSize是这个变长整形的长度
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try{
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1,s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2,s2);
        int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
        if( cmp==0 ) {
          return TEXT_COMPARATOR.compare(b1, s1+firstL1, l1-firstL1, b2, s2+firstL2, l2-firstL2);
        }
        return cmp;
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
    
    //这样的static在什么时候运行？
    static {
      WritableComparator.define(TextPair.class, new TextPairComparator());
    }
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    CustomizedWritable instance = new CustomizedWritable();
    TextPair tp1 = instance.new TextPair("hello", "hadoop");
    TextPair tp2 = instance.new TextPair("hello", "hadoop");
    System.out.println("Content: "+ tp1.toString());
    System.out.println("Compare: "+ tp1.compareTo(tp2));
    
    byte[] b1 = WritableBasics.serialize(tp1);
    TextPair tp3 = instance.new TextPair();
    WritableBasics.deserialize(tp3, b1);
    System.out.println("Content: "+ tp3.toString());
    byte[] b3 = WritableBasics.serialize(tp3);
    
    //WritableComparator.define(TextPair.class, new TextPairComparator());
    //TextPairComparator hehe = new TextPairComparator();
    TextPairComparator comparator = (TextPairComparator) WritableComparator.get(TextPair.class);
    System.out.println("Compare: "+ comparator.compare(b1, 0, b1.length, b3, 0, b3.length));

  }

}
