package com.blockchain.test;

import com.blockchain.app.BinaryFileBufferTxIn;
import com.blockchain.app.BinaryFileBufferTxOut;
import com.blockchain.app.ReadPropFromLocal;
import com.google.code.externalsorting.ExternalSort;
import com.google.code.externalsorting.IOStringStack;

import java.io.*;
import java.nio.charset.Charset;
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
public class PreProcessing {
    public static void main(String[] args) throws Exception {
        /**Step 1 */
        removeDuplicatesfromFile(new File("E:/hw2/lan/txout.txt")
                ,new File("E:/hw2/lan/singleOpTxn.txt"));
        /**Step 2 */
        List<File> files = new ArrayList<>();
        files.add(new File("E:/hw2/lan/txin.txt"));
        files.add(new File("E:/hw2/lan/singleOpTxn.txt"));
        Comparator<String> cmp = (op1, op2) ->
                new Integer(op1.split("\t")[0]).compareTo(new Integer(op2.split("\t")[0]));
        mergeSortedFiles(files, new File("E:/hw2/lan/mergeTxnIpSingleOp.txt"),cmp,false);
        /**Step 3 */
        getEdgeList(new File("E:/hw2/lan/mergeTxnIpSingleOp.txt"),new File("E:/hw2/lan/addr_edges.txt"));
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
    public static void getEdgeList(File inputFile,File outputFile) throws Exception {
        //Integer txl=new Integer(0),addrl=new Integer(0),addrl2=new Integer(0);
        final BufferedReader bri = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputFile)));
        String firstLine = bri.readLine();
        Integer txl=new Integer(firstLine.split("\t")[0]);
        Integer addrl=new Integer(firstLine.split("\t")[1]);
        Integer addrl2=new Integer(addrl);
        bri.close();
        final BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputFile)));
        FileOutputStream fos = new FileOutputStream(outputFile);
        final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
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
                txl = line_txID;        //new txID and addID
                addrl = line_addID;
            }
            addrl2 = line_addID;    //prev addID
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
    public static long mergeSortedFiles(List<File> infile, File outfile,
                                        final Comparator<String> cmp, boolean distinct) throws IOException {
        ArrayList<IOStringStack> bfbs = new ArrayList<IOStringStack>();
        BufferedReader brIn = new BufferedReader(
                new InputStreamReader(new FileInputStream(infile.get(0)), Charset.defaultCharset()));
        BinaryFileBufferTxIn bfbIn = new BinaryFileBufferTxIn(brIn);
        bfbs.add(bfbIn);
        BufferedReader brOut = new BufferedReader(
                new InputStreamReader(new FileInputStream(infile.get(1)), Charset.defaultCharset()));
        BinaryFileBufferTxOut bfbOut = new BinaryFileBufferTxOut(brOut);
        bfbs.add(bfbOut);
        BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(outfile, true), Charset.defaultCharset()));
        long rowcounter = ExternalSort.mergeSortedFiles(fbw, cmp, distinct, bfbs);
//        for (File f : infile)
//            f.delete();
        return rowcounter;
    }
    /**
     * This finds the single o/p transactions
     *
     */
    public static void removeDuplicatesfromFile(File inputFile, File outputFile) throws IOException {
        int count=1;
        final BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputFile)));
        FileOutputStream fos = new FileOutputStream(outputFile);
        final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
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

