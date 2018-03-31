package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;


public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    String[] remotePort = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    String current_port = ""; // 5554
    String predecessor_port = "";
    String successor_port = "";
    BlockingQueue<String> queue;


    ArrayList<Node> nodes = new ArrayList<Node>();
    Map<String,String> hmap = new HashMap<String, String>();

    ContentResolver contentProvider ;

    class Node{
        String portNumber; // store the port number e.g. 5554, 5556
        String hashValue;   // hashed value of port number
        String predecessor; // pred port number e.g. 5554 etc.
        String successor;   // succ port number e.g. 5554 etc.

        public Node(String portNumber,String hashValue, String predecessor,String successor){
            this.portNumber = portNumber;
            this.hashValue = hashValue;
            this.predecessor = predecessor;
            this.successor = successor;
        }
    }

    public String dividePortNumBy2(String portNum) {
        return Integer.toString(Integer.parseInt(portNum) / 2);
    }

    public String multiplyPortNumBy2(String portNum){
        return Integer.toString(Integer.parseInt(portNum) * 2);
    }

    public void displayArrayList(){
        StringBuilder alist = new StringBuilder();
        StringBuilder blist = new StringBuilder();
        StringBuilder clist = new StringBuilder();
        for (int i=0;i<nodes.size();i++){
           alist.append(nodes.get(i).portNumber);
           alist.append("|");
           blist.append(nodes.get(i).predecessor);
           blist.append("|");
           clist.append(nodes.get(i).successor);
           clist.append("|");

        }
       // Log.d("SortedArrayList", alist.toString());
       // Log.d("Predecessors",blist.toString());
       // Log.d("Successors",clist.toString());

    }

    public void assignpd(){
       // Log.d("assignpd","This is called1");
        if(nodes.size()==1){
            nodes.get(0).successor = nodes.get(0).portNumber;
            nodes.get(0).predecessor = nodes.get(0).portNumber;

            predecessor_port = nodes.get(0).predecessor;
            successor_port = nodes.get(0).successor;
        }

        if(nodes.size()>=2) {
           // Log.d("assignpd","This is called2");
            for (int i = 0; i < nodes.size(); i++) {

                if (i == 0) {
                    nodes.get(i).successor = nodes.get(i + 1).portNumber;
                    nodes.get(i).predecessor = nodes.get(nodes.size() - 1).portNumber;
                } else if (i == nodes.size() - 1) {
                    nodes.get(i).successor = nodes.get(0).portNumber;
                    nodes.get(i).predecessor = nodes.get(i - 1).portNumber;
                } else {
                    nodes.get(i).successor = nodes.get(i + 1).portNumber;
                    nodes.get(i).predecessor = nodes.get(i - 1).portNumber;
                }
              //  Log.d("assignpd", "just before last if");
                if (nodes.get(i).portNumber.equals(current_port)) {

                    predecessor_port = nodes.get(i).predecessor;
                    successor_port = nodes.get(i).successor;
                    Log.d("assignpd","Current: "+ current_port  +"Pred: " + predecessor_port+ "Succ:" + successor_port);
                }
            }
        }

    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        FileOutputStream outputStream;
        Log.d("Insert","Method Called");

        String filename = (String)values.get("key");
        String string = (String) values.get("value");
        String keyHash ="";

        try {
            keyHash =  genHash(filename);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.d("CurrentPort: "+ current_port ,"hash_value:" +hmap.get(current_port));
        Log.d("predecessorPort: "+ predecessor_port ,"hash_value:" +hmap.get(predecessor_port));
        Log.d("SuccessorPort: "+ successor_port ,"hash_value:" +hmap.get(successor_port));
        Log.d("Key: "+ filename ,"hash_value:" + keyHash);


//        if(nodes.size()==1){
//
//            try {
//                // Log.d("NodeID:"+current_port,filename  + string);
//                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
//                outputStream.write(string.getBytes());
//                outputStream.close();
//                Log.d("insert_method","File write successful in current Node");
//            } catch (Exception e) {
//                Log.e("insert_method", "File write failed");
//            }
//
//        }

        String cpHash = hmap.get(current_port);
        String predHash = hmap.get(predecessor_port);

        if(nodes.size()==1 || ((cpHash.compareTo(predHash) > 0 &&
                keyHash.compareTo(predHash) > 0 &&
                cpHash.compareTo(keyHash) > 0) ||
                ((predHash.compareTo(cpHash) > 0) &&
                        (keyHash.compareTo(predHash) > 0||
                                cpHash.compareTo(keyHash) > 0)))){


            try {
               // Log.d("NodeID:"+current_port,filename  + string);
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
                Log.d("insert_method","File write successful in current Node");
            } catch (Exception e) {
                Log.e("insert_method", "File write failed");
            }
            Log.d("Inserted in PortNumber",current_port);
        }

        else{
            Log.d("Send2Others:CurPort:"+current_port,filename  + string);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Store", current_port,filename,string);

        }

       // Log.d("Insert","Success");
       // Log.d("insert", values.toString());
        return uri;

    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d(TAG,"This port is: " + myPort);
        current_port = portStr;

        contentProvider = getContext().getContentResolver();

        for(int i = 0; i < remotePort.length;i++){
            try {
                String portNum = dividePortNumBy2(remotePort[i]);
                String hvalue = genHash(portNum);
                hmap.put(portNum,hvalue);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        displayArrayList();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.d(TAG, "Can't create a ServerSocket");
            //return;
        }

        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Oncreate", myPort);

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
      Log.d("Query","Reached the method");

        String cpHash = hmap.get(current_port);
        String predHash = hmap.get(predecessor_port);
        String keyHash ="";
        StringBuilder sb;

        try {
            keyHash =  genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(nodes.size()==1){
            Log.d("Query","Node Size1");
            sb = new StringBuilder();
            try {

                FileInputStream input = getContext().openFileInput(selection);
                InputStreamReader input_reader = new InputStreamReader(input);
                BufferedReader br = new BufferedReader(input_reader);

                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);

                }

            } catch (Exception e) {
                // Log.d("Cursor", "File_reading_failed");
            }

            String[] matrixColumns = {"key", "value"};
            MatrixCursor mco = new MatrixCursor(matrixColumns);
            mco.addRow(new Object[]{selection, sb.toString()});
            mco.close();
            return mco;

        }

        else {
            if (((cpHash.compareTo(predHash) > 0 &&
                    keyHash.compareTo(predHash) > 0 &&
                    cpHash.compareTo(keyHash) > 0) ||
                    ((predHash.compareTo(cpHash) > 0) &&
                            (keyHash.compareTo(predHash) > 0 ||
                                    cpHash.compareTo(keyHash) > 0)))) {

                Log.d("Query", "Reached 1st if block");
                sb = new StringBuilder();
                try {

                    FileInputStream input = getContext().openFileInput(selection);
                    InputStreamReader input_reader = new InputStreamReader(input);
                    BufferedReader br = new BufferedReader(input_reader);

                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append(line);
                    }

                } catch (Exception e) {
                    // Log.d("Cursor", "File_reading_failed");
                }

                if (selectionArgs == null) {
                    Log.d("Query", "Condition1");

                    String[] matrixColumns = {"key", "value"};
                    MatrixCursor mco = new MatrixCursor(matrixColumns);
                    mco.addRow(new Object[]{selection, sb.toString()});
                    Log.d("Query", "Condition1 : key: " + selection + " " + sb.toString() + " end");
                    mco.close();
                    return mco;

                } else {
                    Log.d("Query", "Condition2" + "Selection args:" + selectionArgs[0]);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QueryResult", selectionArgs[0], sb.toString());
                    return null;
                }
            }

            else {
                Log.d("Condition3","CP:" + current_port);
                if (selectionArgs == null) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query", current_port, selection);
                    Log.d("Query", "Condition3  : Before Blocking Queue");
                    queue = new ArrayBlockingQueue<String>(1);
                    String ans = null;
                    sb = new StringBuilder();
                    try {
                        ans = queue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Log.d("Query", "Condition3  : After Blocking Queue:ans "+ans);
                    sb.append(ans);

                    String[] matrixColumns = {"key", "value"};
                    MatrixCursor mco = new MatrixCursor(matrixColumns);
                    mco.addRow(new Object[]{selection, sb.toString()});
                    mco.close();

                    return mco;

                } else {
                    Log.d("Query", "Condition4");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query", selectionArgs[0], selection);
                    return null;
                }
            }
        }

        // Log.d("cursor", "added row");
       // Log.v("query", selection);

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
                    Socket socket = serverSocket.accept();

                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String message = in.readLine();

                  //  Log.d("SERVER", message);

                    String[] result = message.split("#");
                    Log.d("Server: Result String",Arrays.toString(result));



                    if(result[2].equals("Oncreate")){
                        Node node1 = new Node (result[1], hmap.get(result[1]), null, null);
                        nodes.add(node1);
                        Collections.sort(nodes, new Comparator<Node>() {
                            @Override
                            public int compare(Node lhs, Node rhs) {
                                return lhs.hashValue.compareTo(rhs.hashValue);
                            }
                        });

                        assignpd();
                        displayArrayList();
                    }

                    if(result[2].equals("Store")){

                        String key = result[0];
                        String value = result[1];

                        ContentValues contentValues1 = new ContentValues();
                        contentValues1.put("key", key);
                        contentValues1.put("value", value);
                        contentProvider.insert(mUri,contentValues1);
                    }

                    if(result[2].equals("Query")){

                        Log.d("ServerQuery","ResultArray"+Arrays.toString(result));
                        String[] portnum = new String[1];
                        portnum[0] = result[0];

                        String key = result[1];

                        contentProvider.query(mUri, null,
                                key, portnum, null);
                    }

                    if(result[2].equals("QueryResult")){
                        String msgString = result[1];
                        try {
                            queue.put(msgString);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    in.close();
                    socket.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }


    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Log.d("ClientTask", "Reached ClientTask Method " + Arrays.toString(msgs));

                if(msgs[0].equals("Oncreate")){
                    for (int i = 0; i < remotePort.length; i++) {
                   //     Log.d("Client",remotePort[i]);
//                        if (msgs[1].equals(remotePort[i])) {
//                            Log.d("here","here!!!");
////                            continue;
//                        }
//                        else {
                     //       Log.d("Sending to port:", remotePort[i]);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePort[i]));

                            //First#5554#Oncreate
                            String msgToSend = "First#" + dividePortNumBy2(msgs[1]) + "#Oncreate";

                            DataOutputStream out =
                                    new DataOutputStream(socket.getOutputStream());
                            out.writeBytes(msgToSend+"\n");
                            out.flush();
                            out.close();
                            socket.close();
//                        }
                    }
                }

                if(msgs[0].equals("Store")) {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(multiplyPortNumBy2(successor_port)));
                    //
                    String msgToSend = msgs[2] +"#" + msgs[3] + "#" + "Store";
                    Log.d("Client-Store",msgToSend);
                    DataOutputStream out =
                            new DataOutputStream(socket.getOutputStream());
                    out.writeBytes(msgToSend+"\n");
                    out.flush();
                    out.close();
                    socket.close();
                }

                if(msgs[0].equals("Query")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(multiplyPortNumBy2(successor_port)));


                    String msgToSend = msgs[1] + "#" + msgs[2] + "#" + "Query";
                    Log.d("Client-Query",msgToSend);
                    DataOutputStream out =
                            new DataOutputStream(socket.getOutputStream());
                    out.writeBytes(msgToSend+"\n");
                    out.flush();
                    out.close();
                    socket.close();
                }

                if(msgs[0].equals("QueryResult")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(multiplyPortNumBy2(msgs[1])));

                    String msgToSend = "Fourth#" +  msgs[2] +"#QueryResult";
                    Log.d("Client-QueryResult",msgToSend);
                    DataOutputStream out =
                            new DataOutputStream(socket.getOutputStream());
                    out.writeBytes(msgToSend+"\n");
                    out.flush();
                    out.close();
                    socket.close();

                }

            } catch(UnknownHostException e){
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch(IOException e){
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }
}
