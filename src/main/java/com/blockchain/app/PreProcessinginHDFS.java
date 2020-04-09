package com.blockchain.app;

import com.blockchain.helper.ReadPropFromS3;
import com.google.code.externalsorting.ExternalSort;
import com.google.code.externalsorting.IOStringStack;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * This class is used to pre-process the data for HW2. Part2.
 * *  preprocessing steps
 *      *    1. get all the single o/p transactions from txout.dat
 *      *      - simple - all records are sorted
 *      *      - two pointer problem
 *      *    2. merge all this single o/p transactions with all the i/p transactions
 *      *      - external merge sort - used com.google.externalsort (again an interesting lib - implementation of external merge sort in ADS)
 *      *        - create two Queues for each file - to read chars in bytes (BufferedReader),
 *      *        and store both of them in a Priority Queue, PQ
 *      *        - create another queue (BufferedWriter) to write to a new file
 *      *        - PQ here compares data based on the new Integer(split("\t")[0])
 *      *        - pull smallest record from PQ and write to writer queue
 *      *        - as soon as you pull a record in a queue from PQ, buffer another set of chars into the corresponding queue
 *      *        - Also, go on writing the files into the file from another queue
 *      *    3. Process file and generate the edge list
 *      *        * Example :
 *      *        *   in:
 *      *        *      txID1 + \t + addID1
 *      *        *      txID1 + \t + addID2
 *      *        *   out:
 *      *        *      addID1 + \t + addID2 + \t + txID1
 */
public class PreProcessinginHDFS {
    public static FileSystem fs;

    static {
        try {
            fs = FileSystem.get(new URI(ReadPropFromS3.getProperties("fs.default.name")), new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        /**Step 1 */
        removeDuplicatesfromFile(new Path(ReadPropFromS3.getProperties("readLinesTxout"))
                ,new Path(ReadPropFromS3.getProperties("singleOpTxn")));
        /**Step 2 */
        List<Path> files = new ArrayList<>();
        files.add(new Path((ReadPropFromS3.getProperties("readLinesTxin"))));
        files.add(new Path(ReadPropFromS3.getProperties("singleOpTxn")));
        Comparator<String> cmp = (op1, op2) ->
                new Integer(op1.split("\t")[0]).compareTo(new Integer(op2.split("\t")[0]));
        mergeSortedFiles(files, new Path(ReadPropFromS3.getProperties("mergeTxnIpSingleOp")),cmp,false);
        /**Step 3 */
        getEdgeList(new Path(ReadPropFromS3.getProperties("mergeTxnIpSingleOp")),
                new Path(ReadPropFromS3.getProperties("addr_edges")));
        /**Step 4 */
        /**
         * was unable to run this WINDOWS script through java, in the given time :(
         * but used it to sort and get unique records in the final preprocessed file
         * warning, takes a lot of time {*_*}
         * ->         sort /unique  < E:/hw2/addr_edges.dat > E:/hw2/addr_edges_s.dat
         *
         * */
    }
    /**
     * Process file and generate the edge list
     * Example :
     *   in:
     *      txID1 + \t + addID1
     *      txID1 + \t + addID2
     *   out:
     *      addID1 + \t + addID2 + \t + txID1
     */
    public static void getEdgeList(Path inputFile,Path outputFile) throws Exception {
        Integer txl=new Integer(0),addrl=new Integer(0),addrl2=new Integer(0);
        final BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(inputFile)));
        final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(outputFile)));
        String line;
        while ((line = br.readLine()) != null){
            Integer line_txID = new Integer(line.split("\t")[0]);
            Integer line_addID = new Integer(line.split("\t")[1]);
            if(line_addID.equals(-1))
                continue;
            String writeLine1 = null,writeLine2 = null;
            if(line_txID.equals(txl)){
                if(!addrl.equals(line_addID)) writeLine1= addrl+"\t"+line_addID+"\t"+line_txID+"\n";
                if(!addrl2.equals(line_addID)) writeLine2 = addrl2+"\t"+line_addID+"\t"+line_txID+"\n";
            }
            else{
                txl = line_txID;
                addrl = line_addID;
            }
            addrl2 = line_addID;
            if(writeLine1==null && writeLine2==null);
            else if(writeLine1==null || writeLine2==null){
                if(writeLine1==null) bw.write(writeLine2);
                else bw.write(writeLine1);
            }
            else if (writeLine1.equals(writeLine2))
                bw.write(writeLine1);
            else
                bw.write(writeLine1 + writeLine2);
        }
        bw.close();
    }
    /**
     * This merges txin and txout - writes output file in the format
     * txID + \t + addID
     *
     */
    public static long mergeSortedFiles(List<Path> infile, Path outfile,
                                        final Comparator<String> cmp, boolean distinct) throws IOException {
        ArrayList<IOStringStack> bfbs = new ArrayList<IOStringStack>();
        BufferedReader brIn = new BufferedReader(
                new InputStreamReader(fs.open(infile.get(0))));
        BinaryFileBufferTxIn bfbIn = new BinaryFileBufferTxIn(brIn);
        bfbs.add(bfbIn);
        BufferedReader brOut = new BufferedReader(
                new InputStreamReader(fs.open(infile.get(1))));
        BinaryFileBufferTxOut bfbOut = new BinaryFileBufferTxOut(brOut);
        bfbs.add(bfbOut);
        BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(fs.create(outfile)));
        long rowcounter = ExternalSort.mergeSortedFiles(fbw, cmp, distinct, bfbs);
//        for (File f : infile)
//            f.delete();
        return rowcounter;
    }
    /**
     * This finds the single o/p transactions
     *
     */
    public static void removeDuplicatesfromFile(Path inputFile, Path outputFile) throws IOException {
        int count=0;
        final BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(inputFile)));
        final BufferedWriter bw = new BufferedWriter(
                new OutputStreamWriter(fs.create(outputFile)));
        String line,prevLine = br.readLine();
        while ((line = br.readLine()) != null){
            if(line.split("\t")[0].equals(prevLine.split("\t")[0]))
                count++;
            else{
                if(count==1){
                    bw.write(prevLine);
                    bw.newLine();
                }
                count=1;
                prevLine = line;
            }
        }
        br.close();
        bw.close();
    }
}

