package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClient.DFSInputStream;
import org.apache.hadoop.util.Progressable;

public class DFSClientTest {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost/");
		DFSClient client = new DFSClient(conf);
		
		FileInputStream in = new FileInputStream("result.txt");
		int size = in.available();
		
		int checksum = conf.getInt("io.bytes.per.checksum", -1);
		System.out.println("checksum = " + checksum);
		size = size % checksum == 0 ? size : size + checksum - size % checksum;
		System.out.println("size = " + size);
		byte buffer[] = new byte[1024 * 1024];

		

		BufferedOutputStream out = new BufferedOutputStream(client.create(
				"/home/owner-pc/test/1.txt", true, (short) 1, size));
		
		int length = in.read(buffer);
		while(length > 0){
			out.write(buffer, 0, length);
			length = in.read(buffer);
		}
				
		in.close();
		out.close();
		
		DFSInputStream fin = client.open("/home/owner-pc/test/1.txt");
		
		
		
		BufferedReader reader =new BufferedReader(new InputStreamReader(fin));
		String line = null;
		while((line = reader.readLine()) != null){
			System.out.println(line);
		}
	}
}
