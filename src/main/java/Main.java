import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;

public class Main {


    public static void main(String[] args) throws Exception {

        String hdfsuri = "hdfs://0.0.0.0:8020";
        String path="/user/student/airlines/";
        String inputFileName="airlines.dat";
        String schemaFileName="airlines.avsc";
        String outputFileName="airlines.avro";



        // Init HDFS File System Object
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsuri);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        System.setProperty("HADOOP_USER_NAME", "cloudera");
        System.setProperty("hadoop.home.dir", "/");
        //  Get the filesystem - HDFS
        FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

        Path hdfsFolderPath= new Path(path);


        //Read files
        Path hdfsreadpath = new Path(hdfsFolderPath + "/" + inputFileName);
        FSDataInputStream inputStream = fs.open(hdfsreadpath);
        String out= IOUtils.toString(inputStream,"UTF-8");

        // Convert file to AVRO
        Schema.Parser parser = new Schema.Parser();
        Schema schema=parser.parse(fs.open(new Path(hdfsFolderPath + "/" + schemaFileName)));
        DatumWriter<GenericRecord> writer =new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(schema, fs.create(new Path(outputFileName)));
        dataFileWriter.appendEncoded(ByteBuffer.wrap(out.getBytes()));
        dataFileWriter.close();


//        //Write file
        Path hdfswritepath = new Path(hdfsFolderPath + "/" + outputFileName);
        FSDataOutputStream outputStream=fs.create(hdfswritepath);
        outputStream.writeBytes(outputFileName);
        outputStream.close();
        fs.close();


    }
}