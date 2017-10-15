
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class reader{
    public static void main(String[] args){
        try{
            BufferedReader reader = new BufferedReader(new FileReader("/home/ubuntu/mount_point/rawd/freebase-wex-2009-01-12-articles.tsv"));
            BufferedWriter writter = new BufferedWriter(new FileWriter("/home/ubuntu/1-nRelations.txt"));
            String line = null;
            while((line = reader.readLine()) != null){
                String item[] = line.split("\t");
                String src = item[1];
                String xml = item[3];
                List<String> list = Stringcutter.match(xml);
                if(list.size() == 0){
                    continue;
                }
                String newString = "";
                for(String string : list){
                	if(string.equals(src)){
                		continue;
                	}
                	newString += string + ",";
                }
                writter.write(src + ",1," + newString);

            }
            reader.close();
            writter.close();

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    static class Stringcutter {

        public static List<String> match(String s) {
            List<String> results = new ArrayList<String>();
            Pattern p = Pattern.compile("<target>(.*?)</target>");
            Matcher m = p.matcher(s);
            while (!m.hitEnd() && m.find()) {
                results.add(m.group(1));
            }
            return results;
        }


    }

}
