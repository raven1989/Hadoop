import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

public class AvroBasics {

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(AvroBasics.class.getResourceAsStream("Pair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", new Utf8("LEFT"));
    datum.put("right", new Utf8("RIGHT"));
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(datum, encoder);
    encoder.flush();
    out.close();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    GenericRecord res = reader.read(null, decoder);
    System.out.println("GenericRecord left: "+res.get("left").toString());
    System.out.println("GenericRecord right: "+res.get("right").toString());

  }

}
