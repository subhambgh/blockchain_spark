package com.blockchain.app;

import com.google.code.externalsorting.IOStringStack;

import java.io.BufferedReader;
import java.io.IOException;

public class BinaryFileBufferTxOut implements IOStringStack {

    private BufferedReader fbr;
    private String cache;

    public BinaryFileBufferTxOut(BufferedReader r) throws IOException {
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
        if(line==null)
            this.cache = null;
        else{
            String[] txout = this.fbr.readLine().split("\t");
            this.cache = txout[0]+"\t"+txout[2];
        }
    }
}
