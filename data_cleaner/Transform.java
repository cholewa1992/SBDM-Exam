import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Transform{
    public static void main(String[] args){
        try{
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            BufferedWriter vwriter = new BufferedWriter(
                new OutputStreamWriter(
                    new FileOutputStream("vehicles.csv"), "utf-8")
                );
            BufferedWriter pwriter = new BufferedWriter(
                new OutputStreamWriter(
                    new FileOutputStream("persons.csv"), "utf-8")
                );

            Pattern time = Pattern.compile("<timestep time=\"(.+)\">");

            Pattern vehicle = Pattern
                .compile(
                    "<vehicle id=\"(.+)\" x=\"(.+)\" y=\"(.+)\" angle=\"(.+)\" "+
                    "type=\".+\" speed=\"(.+)\" pos=\"(.+)\" lane=\"(.+)\" slope=\".+\"\\/>"
                );

            Pattern person = Pattern
                .compile(
                    "<person id=\"(.+)\" x=\"(.+)\" y=\"(.+)\" angle=\"(.+)\" "+
                    "speed=\"(.+)\" pos=\"(.+)\" edge=\"(.+)\" slope=\".+\"\\/>"
                );

            String input;
            String step = "empty";

            while((input=br.readLine())!=null){
                Matcher match;
                if((match = vehicle.matcher(input)).find()) {  
                    vwriter.write(
                            match.group(1) + "," +
                            match.group(2) + "," +
                            match.group(3) + "," +
                            match.group(4) + "," +
                            match.group(5) + "," +
                            match.group(6) + "," +
                            match.group(7) + "," +
                            step + "\n"); 
                } 
                else if((match = person.matcher(input)).find()) {  
                    pwriter.write(
                            match.group(1) + "," +
                            match.group(2) + "," +
                            match.group(3) + "," +
                            match.group(4) + "," +
                            match.group(5) + "," +
                            match.group(6) + "," +
                            match.group(7) + "," +
                            step + "\n"); 
                } 
                else if((match = time.matcher(input)).find()) {  
                    step = match.group(1);
                    System.out.println(step);
                }
            }
            pwriter.flush();
            vwriter.flush();
            pwriter.close();
            vwriter.close();
            
        }catch(IOException io){
            io.printStackTrace();
        }
    }
}
