import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;

public class translator {
	public static void main(String[] args){
		HashMap<String, String> hashMap = new HashMap<>();
		try{
			BufferedReader reader = new BufferedReader(new FileReader("/home/ubuntu/prepareUniversityOutput.txt"));
			BufferedReader reader1 = new BufferedReader(new FileReader("/home/ubuntu/mount_point/rawd/freebase-wex-2009-01-12-articles.tsv"));
			String line = null;
			while((line = reader1.readLine()) != null){
				String item[] = line.split("\t");
				String ID = item[0];
				String src = item[1];
				hashMap.put(ID,src);
			}
			reader1.close();
			BufferedWriter writer = new BufferedWriter(new FileWriter("/home/ubuntu/top1000000output.txt"));
			String line1 = null;
			while((line1 = reader.readLine()) != null){
				String item1[] = line1.split(",");
				writer.write(hashMap.get(item1[0]) +"\t" + item1[1]);
				writer.newLine();
			}
			reader.close();
			writer.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
}
