package com.blockchain.app;

import com.google.code.externalsorting.ExternalSort;
import com.google.code.externalsorting.IOStringStack;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.*;

/**
 * This class is used to pre-process the data for HW2. Part2.
 */
public class PreProcessing {
    public static void main(String[] args) throws Exception {
//        Comparator<String> cmp = (op1, op2) ->
//                new Integer(op1.split("\t")[0]).compareTo(new Integer(op2.split("\t")[0]));
//        removeDuplicatesfromFile(new File(ReadPropFromLocal.getProperties("txout"))
//                ,new File("D:/singleOpTxn.txt"));
//        List<File> files = new ArrayList<>();
//        files.add(new File(ReadPropFromLocal.getProperties("txin")));
//        files.add(new File("D:/singleOpTxn.txt"));
//        mergeSortedFiles(files, new File("D:/mergeTxnIpSingleOp.txt"),cmp,false);
        getEdgeList(new File("D:/mergeTxnIpSingleOp.txt"),new File("D:/addr_edges.txt"));
        //sort /unique  < E:/hw2/addr_edges.dat > E:/hw2/addr_edges_s.dat

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
        Integer txl=new Integer(0),addrl=new Integer(0),addrl2=new Integer(0);
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
        int count=0;
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

