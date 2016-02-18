import java.io.ByteArrayOutputStream;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;

public class AvroGeneratePair {

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    Pair datum = new Pair();
    datum.setLeft(new Utf8("LEFT"));
    datum.setRight(new Utf8("RIGHT"));
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<Pair> writer = new SpecificDatumWriter<Pair>(Pair.class);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(datum, encoder);
    encoder.flush();
    out.close();
    
    DatumReader<Pair> reader = new SpecificDatumReader<Pair>(Pair.class);
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    Pair res = reader.read(null, decoder);
    
    System.out.println("Pair :" +res.toString());

  }

}
