import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
//import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
//import org.apache.log4j.*;

/**
 * Solution for Big Data HomeWork 1. Read data from HDFS files line by line.
 * Then, filtering the results and store the rest of rows into HBase.
 * 
 * @author yunhao
 */
public class Hw1Grp5 {
    /**
     * Create table in hbase using a given table name.
     * 
     * @param tableName
     *        The name of table needed to be created
     *
     * @param configuration
     *        A Configuration object for hAdmin
     *
     * @exception IOException
     */
    static void createTable(String tableName, Configuration configuration) throws IOException{
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
        if(hAdmin.tableExists(tableName)){
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
        }
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor cf = new HColumnDescriptor("res");
        htd.addFamily(cf);
        hAdmin.createTable(htd);
        hAdmin.close();
    }

    /**
     * Check if a double number is in range.
     *
     * @param value
     *        The number needed to judge
     *
     * @param selectCondition
     *        Filter operation
     *
     * @param selectOperand
     *        Boundery number of the range
     *
     * @return A boolean value, true for legel, false for illegel
     */
    static boolean isLegel(double value, String selectCondition, double selectOperand){
        if( (selectCondition.equals("gt") && value > selectOperand) || 
            (selectCondition.equals("ge") && value >= selectOperand) || 
            (selectCondition.equals("eq") && value == selectOperand) || 
            (selectCondition.equals("ne") && value != selectOperand) || 
            (selectCondition.equals("le") && value <= selectOperand) || 
            (selectCondition.equals("lt") && value < selectOperand) )
            return true;
        return false;
    }
    
    /**
     * Main procedure
     */
    public static void main(String[] args) throws IOException, 
                                                  URISyntaxException, 
                                                  MasterNotRunningException, 
                                                  ZooKeeperConnectionException {
        // Parse arguments
        String argFilePath = args[0].substring(2);
        int argSelectColume = Integer.parseInt(args[1].substring(args[1].indexOf('R') + 1, args[1].indexOf(',')));
        String argSelectCondition = args[1].substring(args[1].indexOf(',') + 1, args[1].lastIndexOf(','));
        double argSelectOperand = Double.parseDouble(args[1].substring(args[1].lastIndexOf(',') + 1));
        String[] argDistinctColume = args[2].substring(args[2].indexOf(':') + 2).split(",R");
        int[] resultColumeIdx = new int[argDistinctColume.length];
        for(int i = 0; i < argDistinctColume.length; i++)
            resultColumeIdx[i] = Integer.parseInt(argDistinctColume[i]);
        
        // Open file for reading
        FileSystem fileSystem = FileSystem.get(URI.create(argFilePath), new Configuration());
        FSDataInputStream inputStream = fileSystem.open(new Path(argFilePath));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String str;

        // Selection and projection line by line
        ArrayList<String> tmpRes = new ArrayList<String>();
        while((str = bufferedReader.readLine()) != null) {
            String[] columes = str.split("\\|");
            if (!isLegel(Double.parseDouble(columes[argSelectColume]), argSelectCondition , argSelectOperand))
                continue;
            String tmp = "";
            for(int i = 0; i < resultColumeIdx.length; i++){
                if(i != 0) tmp += "|";
                tmp += columes[resultColumeIdx[i]];
            }
            tmpRes.add(tmp);
        }
        bufferedReader.close();
        fileSystem.close();

        // Sort results
        String[] result = new String[tmpRes.size()];
        tmpRes.toArray(result);
        Arrays.sort(result);

        // Create table in HBase
        String tableName= "Result";
        Configuration configuration = HBaseConfiguration.create();
        createTable(tableName, configuration);
        
        // Store results to HBase, distinct operation here
        HTable table = new HTable(configuration, tableName);
        String prev = "";
        Integer rowKey = new Integer(0);
        for(int i = 0; i < result.length; i++){
            if(result[i].equals(prev)) continue;
            String[] columes = result[i].split("\\|");
            for(int j = 0; j < columes.length; j++){
                Put put = new Put(rowKey.toString().getBytes());
                put.add("res".getBytes(), ("R" + argDistinctColume[j]).getBytes(), columes[j].getBytes());
                table.put(put);
            }
            rowKey = new Integer(rowKey.intValue()+1);
            prev = result[i];
        }
        table.close();
    }
}