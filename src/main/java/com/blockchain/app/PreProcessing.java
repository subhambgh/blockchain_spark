package com.blockchain.app;

import com.google.code.externalsorting.BinaryFileBuffer;
import com.google.code.externalsorting.ExternalSort;
import com.google.code.externalsorting.IOStringStack;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.*;

public class PreProcessing {
    public static void main(String[] args) throws Exception {
        Comparator<String> cmp = (op1, op2) ->
                new BigInteger(op1.split("\t")[0]).compareTo(new BigInteger(op2.split("\t")[0]));
        List<File> files = new ArrayList<>();
        String dir = "E:/hw2/local/";
        files.add(new File(dir+"txin.txt"));
        files.add(new File(dir+"txout.txt"));
        mergeSortedFiles(files, new File(dir+"mergeTxnIpOp.txt"),cmp,false);
//        getEdgeList(new File("D:/mergeTxnIpOp.txt"),new File(dir+"addr_edges.dat"));
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
        BigInteger txl=BigInteger.ZERO,addrl=BigInteger.ZERO,addrl2=BigInteger.ZERO;
        final BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputFile)));
        FileOutputStream fos = new FileOutputStream(outputFile);
        final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        String line;
        while ((line = br.readLine()) != null){
            BigInteger line_txID = new BigInteger(line.split("\t")[0]);
            BigInteger line_addID = new BigInteger(line.split("\t")[1]);
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

}

