import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class modifier {
	public static void main(String[] args){
		try {
			BufferedReader reader = new BufferedReader(new FileReader("/home/ubuntu/relations.txt"));
			BufferedWriter writer = new BufferedWriter(new FileWriter("/home/ubuntu/modifiedRelations.txt"));
			String line = null;
			while((line = reader.readLine()) != null){
				String item[] = line.split("\t");
				if(item.length == 1){
					continue;
				}
				String src = item[0];
				String dest = item[1];
				if(dest == null){
					continue;
				}
				if(src.equals(dest)){
					continue;
				}
				writer.write(src + "\t" + dest);
				writer.newLine();
			}
			reader.close();
			writer.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
}
