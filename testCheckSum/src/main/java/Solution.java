import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by sgowda on 09/07/17.
 */
public class Solution {

  public static void main(String args[]) throws Exception  {

    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", "wasb://perf-santhosh-hdi-hotfix-dev-57366@humbtestings2jw.blob.core.windows.net");
    FileSystem fs = FileSystem.get(configuration);
    Path path = new Path("wasb://perf-santhosh-hdi-hotfix-dev-57366@humbtestings2jw.blob.core.windows.net/tmp/santhosh.txt");
    //System.out.println(fs.getAclStatus(path));
    System.out.println(getFile(fs,path));
    System.out.println(fs.getFileChecksum(path));

  }

  public static String getFile(FileSystem fs, Path fileLocation) throws IOException {

    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(fileLocation)));
    StringBuilder sb = new StringBuilder();

    try {
      String line;
      while ((line = bufferedReader.readLine())!= null)   {

        sb.append(line);
        sb.append(System.lineSeparator());
      }
    } finally {
      bufferedReader.close();
    }

    return sb.toString();
  }


}
