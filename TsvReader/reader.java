import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Reader {
	public static void main(String[] args){
		HashMap<String, String> hashMap = new HashMap<>();
		try{
			BufferedReader reader = new BufferedReader(new FileReader("/home/ubuntu/mount_point/rawd/freebase-wex-2009-01-12-articles.tsv"));
			BufferedWriter writer = new BufferedWriter(new FileWriter("/home/ubuntu/relations.txt"));
			String line = null;
			while((line = reader.readLine()) != null){
				String item[] = line.split("\t");
				String src = item[1];
				Pattern pattern1 = Pattern.compile("<target>");
				Matcher matcher1 = pattern1.matcher(item[3]);
				Pattern pattern2 = Pattern.compile("</target>");
				Matcher matcher2 = pattern2.matcher(item[3]);
				
				boolean flag = true;
				while(flag){
					try{
						matcher1.find();
						matcher2.find();
					    int startIndex = matcher1.end();
					    int endIndex = matcher2.start();
					String dest = item[3].substring(startIndex, endIndex);
					//hashMap.put(src, dest);
					//System.out.println(src + "\t" + dest);
					writer.write(src + "\t" + dest);
					writer.newLine();
					}catch(Exception e){
						flag = false;
					}
				}
				
			}
			reader.close();
			writer.close();
		}catch(Exception e){
			e.printStackTrace(); 
		}
//		try{
//			BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/qiaochu/output.txt"));
//			for(String srcString : hashMap.keySet()){
//				writer.write(srcString + "\t" + hashMap.get(srcString));
//				writer.newLine();
//			}
//			writer.close();
//		}catch(Exception e){
//			System.out.println(e.getMessage());
//		}
		System.out.println("finish!");
	}
}
