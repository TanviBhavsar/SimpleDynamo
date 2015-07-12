package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by tanvi on 4/12/15.
 */
public class messageClass {
    public String type;
    public String key;
    public String value;
    public String sourcePort;
    public String destinationPort;
    public String nextNodePort;
    public  String previousNodePort;
    public  String sourceID;
    public  String replicaID;
    messageClass()
    {
        value=null;destinationPort=null;
    }
}

