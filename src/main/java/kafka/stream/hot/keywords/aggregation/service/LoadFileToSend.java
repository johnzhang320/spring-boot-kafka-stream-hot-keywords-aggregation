package kafka.stream.hot.keywords.aggregation.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.Scanner;
@Service
@Slf4j
public class LoadFileToSend {
    //read the “fileTest.txt” available under src/main/resources

    public String readFileFromResource(String testFile) {
        String retVal="nothing";
        try {
           // Create a file object
            File f = new File(testFile);

            // Get the absolute path of file f
            String absolute = f.getAbsolutePath();

            // Display the file path of the file object
            // and also the file path of absolute file
            System.out.println("Original path: "
                    + f.getPath());
            System.out.println("Absolute path: "
                    + absolute);

         InputStream inputStream = new FileInputStream(f);
         retVal = readFromInputStream(inputStream);
        } catch (IOException e) {
            log.info("Failed to read file "+testFile);
            throw new RuntimeException("Failed read file:"+testFile);
        }
        return retVal;
    }
    private String readFromInputStream(InputStream inputStream)
            throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        BufferedReader br = null;
        try  {
             br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        } catch (IOException e) {
            log.info("Failed to read file ");
        } finally {
            if (br!=null) {
                br.close();
            }
            inputStream.close();
        }
        return resultStringBuilder.toString();
    }
}
