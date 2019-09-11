package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

import static android.content.Context.TELEPHONY_SERVICE;

public class SimpleDhtProvider extends ContentProvider {

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String JOIN = "join";
    private static final String LIST_UPDATE = "listUpdate";
    private static final String GIVELOCAL = "giveLocal";
    private static final String GIVEKEY ="givekey";
    private static final String INSERTONCE = "insertOnce";
    private static final String STS = "sendToSuccessor";
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private static final int SERVER_PORT = 10000;
    static final String HEAD_PORT = "11108";
    String REMOTE_PORT0 = "5554";
    String REMOTE_PORT1 = "5556";
    String REMOTE_PORT2 = "5558";
    String REMOTE_PORT3 = "5560";
    String REMOTE_PORT4 = "5562";
    int updateNo = 0;
    HashMap<String, String> portMapReverse = new HashMap<String, String>();
    String[] CLIENT_PORTS = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    int NODE_COUNT = 0;
    String myPort;
    String predPort;
    String succPort;
    String node_ID;
    String currentNode = "";
    ArrayList<String> nodeIDList = new ArrayList<String>();
    String nodeList = "";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals("*") || selection.equals("@"))
        {
            File[] list  = getContext().getFilesDir().listFiles();
            for (File file : list) {
                file.delete();
            }
        }else{
            File dir =  getContext().getFilesDir();
            File file= new File(dir,selection);
            file.delete();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String mFile = (String) values.get(KEY_FIELD);
        String mContent = (String) values.get(VALUE_FIELD);

        Log.e("insert", "Predecessor" + predPort + " , Successor" + succPort);
        try {
            callInsert(mFile,mContent);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return uri;
    }

    //callInsert function to insert at proper place
    private void callInsert(String key, String value) throws NoSuchAlgorithmException {
        String keyHash = genHash(key);
        System.out.println("Key = " + key);
        System.out.println("keyHash = " + keyHash);
        String predHash = genHash(Integer.toString(Integer.parseInt(predPort)/2));
        //if Key belongs to ME
        if(keyHash.compareTo(node_ID) < 0 && keyHash.compareTo(predHash) > 0 && predHash.compareTo(node_ID) < 0){
            Log.e("callInsert","between me and pred");
            insertToNode(key,value);
        }
        //If key belongs to me Across the ring
        else if (((predHash.compareTo(node_ID) > 0) && ((keyHash.compareTo(predHash) > 0) && (predHash.compareTo(keyHash)<0))) || ((predHash.compareTo(node_ID) > 0) && ((node_ID.compareTo(keyHash) > 0) && (predHash.compareTo(keyHash)>0))))
        {
            Log.e("callInsert","edgeCondition1");
            insertToNode(key, value);
        }
        //if only node, add key to myself
        else if(isOnlyNode()){
            Log.e("callInsert","isOnlyNode()");
            insertToNode(key,value);
        }
        //doesn't belong to me, check with successor
        else {
            Log.e("callInsert","sendToSuccessor");
            sendToSuccessor(key,value,succPort);
        }
    }
    //Insert to Myself
    private  void insertToNode(String key, String value){
        try {
            System.out.println("Inserting to "+myPort+"-"+key+" "+value);
            Log.e("insertToNode","Inserting to "+myPort+"-"+key+" "+value);
//            keyInsertedList.add(key);
            FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //Insert to Successor
    private void sendToSuccessor(String key, String value, String port){
//        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, STS, key, value);
        try {
            Socket socketToSucc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(succPort));
            PrintWriter msgSend = new PrintWriter(socketToSucc.getOutputStream(),true);
            Log.i("ClientTask STS","Sending to "+succPort+" : " + STS + "<>" + key+ "<>" + value);
            msgSend.println(STS + "<>" + key+ "<>" + value);
            msgSend.flush();
            msgSend.close();
            socketToSucc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        //Create a reverse hash map
        for (String port : CLIENT_PORTS) {
            try {
                portMapReverse.put(genHash(port), Integer.toString(Integer.parseInt(port)*2));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        System.out.println(portMapReverse);
        //create server port
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.i("onCreate","ServerSocket Created");

            TelephonyManager telManager = (TelephonyManager) getContext().getSystemService(TELEPHONY_SERVICE);
            String portString = telManager.getLine1Number().substring(telManager.getLine1Number().length() - 4);
            final String portNumber = String.valueOf((Integer.parseInt(portString)) * 2);
            currentNode = portString;
            node_ID = genHash(currentNode);
            Log.v("onCreate", "My node_ID = " + node_ID);
            myPort = portNumber;
            predPort = myPort;
            succPort = myPort;
            Log.i("onCreate","currentNode = "+currentNode);

            //Check if not headPort
            if (!myPort.equals(HEAD_PORT)) {
                //call client task to join
                String joinMSg = JOIN + "<>" + myPort + "<>" + node_ID;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, JOIN, myPort, node_ID);
            }
            //if head port add to List
            else {
                nodeIDList.add(node_ID);
                NODE_COUNT++;
                Log.i("onCreate","nodeIDList = " + nodeIDList.toString());
            }

        } catch (IOException e) {
            Log.e("onCreateException",e.toString());
            Log.e("onCreateException", "Can't create a ServerSocket");
            return false;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return true;
    }

    //Function to check if its the only node
    public boolean isOnlyNode() {
        return predPort.equals(myPort) && succPort.equals(myPort);
    }

    //return cursor to all of my keys
    private MatrixCursor queryMyselfForAll(){
        FileInputStream inputStream;
        String[] mColumnNames = {KEY_FIELD, VALUE_FIELD};
        String mFileContent;
        byte[] content = new byte[50];
        MatrixCursor matrixCursor = new MatrixCursor(mColumnNames);
        String[] Flist = getContext().fileList();
//        Log.i("queryMyselfForAll", Flist.);
        System.out.println("FileList = "+Arrays.toString(Flist));
        for(String key : Flist){
            Log.i("Query", "Looking for key :" + key);
            try {
                inputStream = getContext().openFileInput(key);
                int length = inputStream.read(content);
                mFileContent = new String(content).substring(0, length);
                MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
                mRowBuilder.add(mColumnNames[0], key);
                mRowBuilder.add(mColumnNames[1], mFileContent);
                inputStream.close();
            } catch (Exception e) {
                Log.e("queryFileException", e.toString());
            }
        }
        return matrixCursor;
    }

    //return string to all my keys
    private String queryMyselfForAllString(){
        String retString = "";
        FileInputStream inputStream;
        String[] mColumnNames = {KEY_FIELD, VALUE_FIELD};
        String mFileContent;
        byte[] content = new byte[50];
        String[] Flist = getContext().fileList();
        System.out.println("FileList = "+Arrays.toString(Flist));
        for(String key : Flist){
            Log.i("Query", "Looking for key :" + key);
            try {
                inputStream = getContext().openFileInput(key);
                int length = inputStream.read(content);
                mFileContent = new String(content).substring(0, length);
                retString+= key+":"+mFileContent+"---";
                inputStream.close();
            } catch (Exception e) {
                Log.e("queryFileException", e.toString());
            }
        }
        return retString;
    }

    //return cursor to sent key,value pair
    private MatrixCursor queryMyselfForOne(String key){
        FileInputStream inputStream;
        String[] mColumnNames = {KEY_FIELD, VALUE_FIELD};
        String mFileContent;
        byte[] content = new byte[50];
        MatrixCursor matrixCursor = new MatrixCursor(mColumnNames);
        try {
            inputStream = getContext().openFileInput(key);
            int length = inputStream.read(content);
            mFileContent = new String(content).substring(0, length);
            MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
            mRowBuilder.add(mColumnNames[0], key);
            mRowBuilder.add(mColumnNames[1], mFileContent);
            inputStream.close();
        } catch (Exception e) {
            Log.e("queryFileException", e.toString());
        }
        Log.i("query", "count = " + matrixCursor.getCount());
        Log.v("query", "selection = " + key);
        return matrixCursor;
    }

    //return string with value of key
    private String queryMyselfForOneString(String key){
        FileInputStream inputStream;
        String[] mColumnNames = {KEY_FIELD, VALUE_FIELD};
        String retString;
        String mFileContent;
        byte[] content = new byte[50];
//        MatrixCursor matrixCursor = new MatrixCursor(mColumnNames);
        try {
            inputStream = getContext().openFileInput(key);
            int length = inputStream.read(content);
            mFileContent = new String(content).substring(0, length);
            retString = mFileContent;
            inputStream.close();
            return retString;
        } catch (Exception e) {
            Log.e("queryFileException", e.toString());
        }
//        Log.i("query", "count = " + matrixCursor.getCount());
//        Log.v("query", "selection = " + key);
        return "NotFound";
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
//        if(selection.equals("*") <> selection.equals("@")) {
        System.out.println("Selection = " + selection);

        //if Only Node
        if(isOnlyNode()) {
            if ((selection.equals("*") || selection.equals("@"))) {
                Log.i("Query :", "SingleNode First");
                MatrixCursor returnCursor = queryMyselfForAll();
                Log.i("query", "count = " + returnCursor.getCount());
                return returnCursor;
            }
            else {
                Log.i("Query :","Single Key else");
                MatrixCursor returnCursor = queryMyselfForOne(selection);
                Log.i("query", "count = " + returnCursor.getCount());
                return returnCursor;
            }
        }
        //if more than one nodes
        else{
            if(selection.equals("@")){
                Log.i("Query :", "SingleNode First");
                MatrixCursor returnCursor = queryMyselfForAll();
                Log.i("query", "count = " + returnCursor.getCount());
                return returnCursor;
            }
            else if(selection.equals("*")){
                Log.i("Query :", "SingleNode First");
                String myALL = queryMyselfForAllString();
                String others = getLocalFromSuccessors(myPort);
                String total = myALL + others;
                MatrixCursor returnCursor = getCursorFromString(total);
                Log.i("query", "count = " + returnCursor.getCount());
                return returnCursor;
            }
            else {
                String value = queryMyselfForOneString(selection);
                if(value.equals("NotFound")){
                    value = querySuccessors(selection,myPort);
                }
                String[] mColumnNames = {KEY_FIELD, VALUE_FIELD};
                MatrixCursor retCursor = new MatrixCursor(mColumnNames);;
                MatrixCursor.RowBuilder mRowBuilder = retCursor.newRow();
                mRowBuilder.add(mColumnNames[0], selection);
                mRowBuilder.add(mColumnNames[1], value);
                return retCursor;
            }
        }
    }

    //return query to my successor
    private String querySuccessors(String key, String fromPort){
        String retSring = null;
        String succ = succPort;
        while (!succ.equals(fromPort) ){
            try {
                Socket portToSucc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(succ));
                PrintWriter msgSend = new PrintWriter(portToSucc.getOutputStream(),true);
                BufferedReader fromServer = new BufferedReader(new InputStreamReader(portToSucc.getInputStream()));
                msgSend.println(GIVEKEY+"<>"+key);

                String msgBack;
                if((msgBack = fromServer.readLine()) != null){
                    String[] msgParts = msgBack.split("<>");
                    succ = msgParts[0];
                    retSring = msgParts[1];
                }
                msgSend.flush();
                msgSend.close();
                portToSucc.close();
                if(!retSring.equals("NotFound")) {
                    return retSring;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Log.i("getLocalFromSuccessors","retSring = "+retSring);
        return retSring;
    }

    //get local from successor
    private String getLocalFromSuccessors(String fromPort){
        String retSring = "";
        String succ = succPort;
        while (!succ.equals(fromPort)){
            try {
                Socket portToSucc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(succ));
                PrintWriter msgSend = new PrintWriter(portToSucc.getOutputStream(),true);
                BufferedReader fromServer = new BufferedReader(new InputStreamReader(portToSucc.getInputStream()));
                msgSend.println(GIVELOCAL);

                String msgBack;
                if((msgBack = fromServer.readLine()) != null){
                    Log.e("give local", msgBack);
                    String[] msgParts = msgBack.split("<>");
                    if(msgParts.length == 2) {
                        succ = msgParts[0];
                        retSring += msgParts[1];
                    }
                    else succ = msgParts[0];
                }
                msgSend.flush();
                msgSend.close();
                portToSucc.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Log.i("getLocalFromSuccessors","retSring = "+retSring);
        return retSring;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    //send list update to everyone
    private void sendJoinUpdate(String list1) {
        for (String remote_port : list1.split("->")) {
            if (remote_port.equals(HEAD_PORT)) {
                Log.i("ClientTask Update", "myPort");
                int myPos = nodeIDList.indexOf(node_ID);
                int n = nodeIDList.size();
                String predPortHash = nodeIDList.get((n + myPos - 1) % n);
                String succPortHash = nodeIDList.get((n + myPos + 1) % n);
                predPort = portMapReverse.get(predPortHash);
                succPort = portMapReverse.get(succPortHash);
                Log.v("NewJoin", "PortOrder after NewJoin : " + predPort + "->" + myPort + "->" + succPort);
            }
            else {
                try {
//                    Log.i("ClientTask Update", "remote_port = " + remote_port);
                    Socket clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remote_port));
                    PrintWriter sendList = new PrintWriter(clientSocket.getOutputStream(),true);
                    String msg = LIST_UPDATE + "<>" + list1;
                    Log.i("ClientTask Update", "Sending to " + remote_port + " : " + msg);
                    sendList.println(msg);
                    sendList.flush();
                    sendList.close();
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    //function to convert hash to string
    private String getListFromHash(ArrayList<String> nodeIDList){
        StringBuilder returnString = new StringBuilder();
        for (String hashes : nodeIDList){
            String port = portMapReverse.get(hashes);
            returnString.append(port);
            returnString.append("->");
        }
        return returnString.toString();
    }

    //function to return matrix cursor from string
    private MatrixCursor getCursorFromString(String str){
        String[] mColumnNames = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor returnCursor = new MatrixCursor(mColumnNames);
        String[] keyValuePairs = str.split("---");
        for (String pair : keyValuePairs){
            String[] p = pair.split(":");
            String key = p[0];
            String value = p[1];
            MatrixCursor.RowBuilder mRowBuilder = returnCursor.newRow();
            mRowBuilder.add(mColumnNames[0], key);
            mRowBuilder.add(mColumnNames[1], value);
        }
        return returnCursor;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.i("ServerTask","InServerTask");
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    String msg = "";
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader readMessage = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter msgBackWriter = new PrintWriter(clientSocket.getOutputStream(),true);
                    try {
                        if((msg = readMessage.readLine()) != null){
                            String[] msgParts = msg.split("<>");
                            Log.i("ServerTask","msgType = "+ msgParts[0]);
                            if (msgParts[0].equals(JOIN)) {
                                Log.i("ServerTask 5554", "Msg Received = " + msg+"From port: "+msgParts[1]);
                                String fromPort = msgParts[1];
                                String nodeID = msgParts[2];
                                msgBackWriter.println("ACK");
                                msgBackWriter.flush();
                                msgBackWriter.close();
                                nodeIDList.add(nodeID);
                                Collections.sort(nodeIDList);
                                String nodes = getListFromHash(nodeIDList);
                                sendJoinUpdate(nodes);
                                NODE_COUNT++;
                                System.out.println("NODE_COUNT = "+(NODE_COUNT));
                                Log.i("ServerTask 5554", "End of ServerTask");
                            }
                            else if(msgParts[0].equals(LIST_UPDATE)){
                                updateNo++;
                                nodeList = msgParts[1];
                                Log.i("Update List Server","List = " + nodeList);
                                nodeIDList.clear();
                                for (String portSolo : nodeList.split("->")){
                                    nodeIDList.add(genHash(Integer.toString(Integer.parseInt(portSolo)/2)));
                                }
                                int myPos = nodeIDList.indexOf(node_ID);
                                int n = nodeIDList.size();
                                String predPortHash = nodeIDList.get((n+myPos-1)%n);
                                String succPortHash = nodeIDList.get((n+myPos+1)%n);
                                predPort = portMapReverse.get(predPortHash);
                                succPort = portMapReverse.get(succPortHash);
                                Log.i(LIST_UPDATE,"Updated Hash = " + nodeIDList);
                                Log.v("NewJoin", "PortOrder after NewJoin : " + predPort + "->" + myPort + "->" + succPort);
                            }
                            else if(msgParts[0].equals(INSERTONCE)){
                                String key = msgParts[1];
                                String value = msgParts[2];
                                insertToNode(key,value);
//                                Log.i("INSERTONCE", "Inserted to "+myPort+"key = "+key+"value ="+value);
                            }
                            else if(msgParts[0].equals(STS)){
                                callInsert(msgParts[1],msgParts[2]);
                            }
                            else if(msgParts[0].equals(GIVELOCAL)){
//                                MatrixCursor cursor = queryMyselfForAll();
//                                String s = convertMatrixCursorToString(cursor);
                                String s = queryMyselfForAllString();
//                                if()
                                String msg1 = succPort + "<>" + s;
                                PrintWriter pw = new PrintWriter(clientSocket.getOutputStream());
                                pw.println(msg1);
                                pw.flush();
                                pw.close();
                            }
                            else if(msgParts[0].equals(GIVEKEY)){
                                String key = msgParts[1];
                                String retString = queryMyselfForOneString(key);
                                PrintWriter pwr = new PrintWriter(clientSocket.getOutputStream(),true);
                                pwr.println(succPort + "<>" + retString);
                                pwr.flush();
                                pwr.close();
                            }
                            else Log.e("ServerTask Else","Unhandled Case");
                        }
                    }
                    catch (IOException e){
                        Log.e("ST","IO Exception");
                        Log.e("ST", e.toString());
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    clientSocket.close();
                    readMessage.close();
                    Log.i("ST", "ClientSocket Closed ");
                }
            } catch (IOException e) {
                Log.e("serverTaskBGException", e.toString());
            }
            Log.i("ServerTask","End of ServerTask");
            return null;
        }
        //Reference - https://www.codota.com/code/java/classes/android.content.ContentValues
        @Override
        protected void onProgressUpdate(String... values) {
            super.onProgressUpdate(values);
        }
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... params) {
            String type = params[0];
            Log.i("ClientTask","msgType = "+type);
            if (type.equals(JOIN)) {
                try {
                    String myPort = params[1];
                    String node_ID = params[2];
                    Socket socketToHeadPort = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(HEAD_PORT));
                    PrintWriter writeToHeadPort = new PrintWriter(socketToHeadPort.getOutputStream(), true);
                    BufferedReader backFromHeadPort = new BufferedReader(new InputStreamReader(socketToHeadPort.getInputStream()));
                    String msgToSend = JOIN + "<>" + myPort + "<>" + node_ID;
                    Log.i("ClientTask", "Sending " + msgToSend);
                    //Sent to 5554 JOIN and port that wants to join
                    writeToHeadPort.println(msgToSend);
                    writeToHeadPort.flush();
                    writeToHeadPort.close();
                    while(!socketToHeadPort.isClosed()){
                        String s = backFromHeadPort.readLine();
                        if(s!=null){
                            socketToHeadPort.close();
                            backFromHeadPort.close();
                            break;
                        }
                    }

                } catch (Exception e) {
                    Log.e("clientTaskException", e.toString());
                    Log.e("clientTaskException", "ClientTask Error");
                }
            }
            Log.i("ClientTask","ClientTaskEnd");
            return null;
        }
    }

    //HashFunction
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