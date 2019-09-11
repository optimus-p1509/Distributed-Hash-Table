package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class Node implements Comparable<Node>{
    String myPort;
    String predecessorPort;
    String successorPort;

    public Node(){

    }

    public Node(String myPort){
        this.myPort=myPort;
        this.predecessorPort = myPort;
        this.successorPort = myPort;
    }

    public Node(String myPort, String predecessorPort, String successorPort){
        this.myPort = myPort;
        this.predecessorPort = predecessorPort;
        this.successorPort = successorPort;
    }

    @Override
    public String toString() {
        return myPort + "---" +
                predecessorPort + "---" +
                successorPort;
    }

    @Override
    public int compareTo(Node another) {
        try {
            return (genHash(this.myPort).compareTo(genHash(another.myPort)));
        } catch (NoSuchAlgorithmException e) {
            Log.i("NSA", e.toString());
        }
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
