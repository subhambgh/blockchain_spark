package com.blockchain.app;

import com.google.code.externalsorting.IOStringStack;

import java.io.BufferedReader;
import java.io.IOException;

public class BinaryFileBufferTxIn implements IOStringStack {

    private BufferedReader fbr;
    private String cache;

    public BinaryFileBufferTxIn(BufferedReader r) throws IOException {
        this.fbr = r;
        reload();
    }

    public void close() throws IOException {
        this.fbr.close();
    }

    public boolean empty() {
        return this.cache == null;
    }

    public String peek() {
        return this.cache;
    }

    public String pop() throws IOException {
        String answer = peek().toString();// make a copy
        reload();
        return answer;
    }

    private void reload() throws IOException {
        String line = this.fbr.readLine();
        if (line ==null)
            this.cache = null;
        else {
            String[] txin = line.split("\t");
            this.cache = txin[0]+"\t"+txin[4];
        }

    }


}
