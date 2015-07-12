package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.BufferedOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {


    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    String myPort, myID, myhash, previousHash;
    int myIndex, socketInsertFlag, recoveryFlag;
    List<String> hashPorts;
    static final int SERVER_PORT = 10000;
    int send_ctr = 0;
    TreeMap<String, String> idPortMap = new TreeMap<String, String>();
    ArrayList<String> starFileNameList, starFileContentList;
    HashMap<String, String> recoveryMap;
   

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub


       
        ArrayList<String> fileNameList = new ArrayList<String>();
        ArrayList<String> fileContentList = new ArrayList<String>();
        //used developer.android.com/guide/topics/providers/content-provider-basics.html
        if (selection.equals("\"@\"") || (selection.equals("\"*\""))) {
            fileNameList = getAllFiles();

        } else
            fileNameList.add(selection);
        for (String fileName : fileNameList) {
            File f = new File(fileName);
            //deleteFileName has name of file which is to be deleted and is present on this node
            String deleteFileName = checkFile(fileName);
            if (deleteFileName != null)

                deleteFile(deleteFileName);
            else {
                //Log.e(TAG, "File is not present on this node..deleteFileName is null");
                deleteSuccessor(fileName);


            }
            //     fileContentList.add(fileContent);
        }
        if (selection.equals("\"*\"")) {
           /* messageClass newMessage = new messageClass();
            newMessage.sourcePort = myPort;
            newMessage.type = "deleteMessage";
            newMessage.destinationPort = nextPortPointer;
            newMessage.value=selection;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessage);
            while (deleteComplete == 0) {

            }*/
        }
//        return 0;
        //Log.e(TAG, "n  delete: end returning");
        return 0;

    }

    //find actual location of file and location of deleted files
    private void deleteOther(String fileName) {
    }


    private void deleteNext(String fileName) {
    }

    void deleteFile(String selection)

    {
        //Log.e(TAG, "deleteFile:Before deleting file " + selection);
        FileInputStream fos = null;

        Boolean a = getContext().deleteFile(selection);
        //Log.e(TAG, "deleteFile:After deleting file " + selection + "result is" + a);


    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public void passSuccessor(ContentValues values) {

        //Log.e(TAG, "In passSuccessor");


        String key = (String) values.get("key");
        String val = (String) values.get("value");
        String keyHash = null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int flag = 0, destIndex = 0;
        for (int i = 0; i < 5; i++) {


            if (keyHash.compareTo(hashPorts.get(i)) <= 0) {
                flag = 1;
                destIndex = i;
                break;
            }

        }

        messageClass insertMessage = new messageClass();
        insertMessage.type = "insertMessage";
        String destId = idPortMap.get(hashPorts.get(destIndex));

        insertMessage.destinationPort = Integer.toString((Integer.parseInt(destId) * 2));
        insertMessage.value = key + "---" + val;
        messageClass[] Allmessages = new messageClass[1];
        Allmessages[0] = insertMessage;
       
        client_send(Allmessages);
       
        sendReplica(values, destIndex);
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        

        String key = (String) values.get("key");
        String val = (String) values.get("value");
      
        String keyHash = null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int hashIndex;
        
        if (myIndex == 0)
            hashIndex = 4;
        else
            hashIndex = myIndex - 1;

        
        if (keyHash.compareTo(previousHash) > 0 && keyHash.compareTo(myhash) <= 0)
            insertAtSelf(uri, values);
        else if (keyHash.compareTo(myhash) <= 0 && keyHash.compareTo(previousHash) < 0 && myhash.compareTo(previousHash) < 0)
            insertAtSelf(uri, values);
        else if ((keyHash.compareTo(myhash) > 0) && (keyHash.compareTo(previousHash) > 0) && (myhash.compareTo(previousHash) < 0))
            insertAtSelf(uri, values);

        else passSuccessor(values);
        return uri;
        //return null;
    }


    public Uri insertByInput(Uri uri, ContentValues values, Socket insertSocket) {
        // TODO Auto-generated method stub
        //Log.e(TAG, "In insertbtinput");

      
        int hashIndex;
        
        if (myIndex == 0)
            hashIndex = 4;
        else
            hashIndex = myIndex - 1;

       
        insertAtSelfByInput(uri, values);


       
        messageClass insertAck = new messageClass();
        insertAck.value = "insertAck";
        sendMessageSocket(insertSocket, insertAck);
        return uri;
        
    }


    public void insertAtSelf(Uri uri, ContentValues values) {

        
        String key = (String) values.get("key");
        String val = (String) values.get("value");
        String fileNameInsert = "my" + "&" + myID + "&" + myID + "&" + key;
        FileOutputStream fos = null;
        try {

          
            fos = getContext().openFileOutput(fileNameInsert, Context.MODE_PRIVATE);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            
            fos.write(val.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        
        sendReplica(values, myIndex);


    }

    public void insertAtSelfByInput(Uri uri, ContentValues values) {

        String key = (String) values.get("key");
        String val = (String) values.get("value");
        String fileNameInsert = "my" + "&" + myID + "&" + myID + "&" + key;
        FileOutputStream fos = null;
        try {

           

            fos = getContext().openFileOutput(fileNameInsert, Context.MODE_PRIVATE);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
           
            fos.write(val.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        


    }

    void starMessageInput(messageClass inputObject, Socket socketSend) {
    
        ArrayList<String> fileNameList = getAllFiles();
        ArrayList<String> fileContentList = new ArrayList<String>();
        StringBuilder sendValSb = new StringBuilder();
      
        for (String fileName : fileNameList) {
            
            sendValSb.append(fileName);
            sendValSb.append("---");

            String fileContent = readFile(fileName);
            sendValSb.append(fileContent);
            sendValSb.append(":");
           
        }
        messageClass newMessage = new messageClass();
        newMessage.sourcePort = inputObject.sourcePort;
        if (sendValSb.length() > 0)
            newMessage.value = sendValSb.toString();
        
        newMessage.type = inputObject.type;
       
        sendMessageSocket(socketSend, newMessage);
        


    }

    void recoverMessageInput(messageClass inputObject, Socket socketSend) {
   
        ArrayList<String> fileNameList = getAllFiles();
        ArrayList<String> fileContentList = new ArrayList<String>();
        StringBuilder sendValSb = new StringBuilder();
      
        for (String fileName : fileNameList) {
          
            sendValSb.append(fileName);
            sendValSb.append("---");
          
            String fileContent = readFile(fileName);
           
            sendValSb.append(fileContent);
            sendValSb.append(":");
            
        }
        messageClass newMessage = new messageClass();
        newMessage.sourcePort = inputObject.sourcePort;
        if (sendValSb.length() > 0)
            newMessage.value = sendValSb.toString();
       
        newMessage.type = inputObject.type;
        
        sendMessageSocket(socketSend, newMessage);
        


    }


    public void insertReplica(String key, String val, String replicaID, Socket insertSocket) {

        String fileNameInsert = "rep" + "&" + replicaID + "&" + myID + "&" + key;
       
        FileOutputStream fos = null;
        try {


            fos = getContext().openFileOutput(fileNameInsert, getContext().MODE_WORLD_WRITEABLE);
         
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            fos.write(val.getBytes());
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


      
        messageClass insertAck = new messageClass();
        insertAck.value = "insertAck";
        sendMessageSocket(insertSocket, insertAck);


    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub]
        
        socketInsertFlag = 0;
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            myID = Integer.toString(Integer.parseInt(myPort) / 2);
            myhash = genHash(myID);
            hashPorts = new ArrayList<String>();
            for (int i = 5554, ctr = 0; i <= 5562; i = i + 2, ctr++) {
                

                String hashid = genHash(Integer.toString(i));
                if (hashid.equals(myID))
                    myIndex = ctr;
                hashPorts.add(hashid);
                idPortMap.put(hashid, Integer.toString(i));
            }

            Collections.sort(hashPorts);
            for (int p = 0; p < 5; p++) {

               
                if ((hashPorts.get(p).compareTo(myhash)) == 0) {
                    myIndex = p;
                    if (p == 0)
                        previousHash = hashPorts.get(4);
                    else
                        previousHash = hashPorts.get(p - 1);
                    break;
                }
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        recoveryFlag = 1;
        ArrayList<String> listFiles = getAllFiles();
        if (listFiles.size() != 0) {
            recoveryFlag = 0;
          
            processMessageObject processObject = new processMessageObject();
            new RecoveryMessageTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, processObject);
         
        }
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
   -          * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            e.printStackTrace();
           
        }

        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        return false;

    }

    public ArrayList<String> getAllFiles() {
        File f; // current directory
        
        ArrayList<String> fileNameList = new ArrayList<String>();
       
        for (String s : getContext().fileList()) {
            
            fileNameList.add(s);
        }

    
        return fileNameList;

    }

    public String readFile(String selection) {


        
        //used http://developer.android.com/guide/topics/data/data-storage.html
        FileInputStream fos = null;

        try {

            fos = getContext().openFileInput(selection);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        // used http://stackoverflow.com/questions/16282368/concatenate-chars-to-form-string-in-java
        int content;
        StringBuilder sb = new StringBuilder();

      
        String readString = null;
        InputStreamReader inReader = new InputStreamReader(fos);
        BufferedReader brReader = new BufferedReader(inReader);
        try {
            while ((readString = brReader.readLine()) != null) {
                sb.append(readString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String str = sb.toString();
       
        return str;
    }

    public MatrixCursor createMatrixCursor(ArrayList<String> fileNameList, ArrayList<String> fileContentList) {

       
        String[] a = new String[2];
        a[0] = "key";
        a[1] = "value";
        MatrixCursor matrixCursor = new MatrixCursor(a);
        //Used http://stackoverflow.com/questions/9435158/how-to-populate-listview-from-matrix-cursor

        //type#id of node where it shpuld be inserted#id where it is inserted
        // String fileNameInsert = "rep" + "#" + replicaID +"#" + myID+ "#" + key;
        int i = 0;
        String keyFinal;
        for (String selection : fileNameList) {
            if (selection.contains("&")) {
                String[] values = selection.split("&");
                keyFinal = values[3];
            } else
                keyFinal = selection;
            Object[] cv;
            cv = new Object[2];
            cv[0] = keyFinal;
            cv[1] = fileContentList.get(i);
            
            matrixCursor.addRow(cv);
            i++;
        }
       
        return matrixCursor;

    }

    String checkFile(String filename) {
       
        ArrayList<String> filenamelist = getAllFiles();
        for (String str1 : filenamelist) {
            if (str1.contains(filename))
                return str1;
            if (str1.equals(filename))
                return str1;
        }
        return null;
    }

    String queryNode(String fileName, int destIndex) {

        
        messageClass msg = new messageClass();
        msg.type = "queryMessage";
        msg.sourcePort = myPort;
       
        msg.key = fileName;
        String destId = idPortMap.get(hashPorts.get(destIndex));


        msg.destinationPort = Integer.toString((Integer.parseInt(destId) * 2));

        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(msg.destinationPort));
            socket.setSoTimeout(1800);
        } catch (IOException e) {
            e.printStackTrace();
            int destNew = findNextIndex(destIndex);
            
            String fileContentNext = queryNode(fileName, destNew);
            return fileContentNext;
        }
      

        //used http://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                  
        ObjectOutputStream outputStream = null;
        try {
            
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
            outputStream = new ObjectOutputStream(bufferedOutputStream);
          
        } catch (IOException e) {
            e.printStackTrace();
            int destNew = findNextIndex(destIndex);
            
            String fileContentNext = queryNode(fileName, destNew);
            return fileContentNext;
        }

       
        String msgToSend = msg.type + "#" + msg.key + "#" + msg.value + "#" + msg.sourcePort + "#" + msg.destinationPort + "#" + msg.nextNodePort + "#" + msg.previousNodePort + "#" + msg.sourceID + "#" + msg.replicaID;
       

        System.out.println("Object to be written = " + msg);
        
        try {
            outputStream.writeObject(msgToSend);
            
            send_ctr++;
        } catch (IOException e) {
            e.printStackTrace();
            int destNew = findNextIndex(destIndex);
            
            String fileContentNext = queryNode(fileName, destNew);
            return fileContentNext;
        }
        try {
            outputStream.flush();

        } catch (IOException e) {
            e.printStackTrace();
            int destNew = findNextIndex(destIndex);
           
            String fileContentNext = queryNode(fileName, destNew);
            return fileContentNext;
        }
        


        //http://www.coderpanda.com/java-socket-programming-transferring-of-java-objects-through-sockets/
//socket.
        ObjectInputStream inStream = null;
        try {

            

            BufferedInputStream bufferedInputStream=new BufferedInputStream(socket.getInputStream());
            inStream = new ObjectInputStream(bufferedInputStream);
        } catch (IOException e) {

            e.printStackTrace();
            int destNew = findNextIndex(destIndex);
           
            String fileContentNext = queryNode(fileName, destNew);
            return fileContentNext;
        }


        String inputMessage = null;
        try {
            inputMessage = (String) inStream.readObject();
 
        } catch (SocketTimeoutException e) {
            e.printStackTrace();
            int destNew = findNextIndex(destIndex);
        
            String fileContentNext = queryNode(fileName, destNew);
            return fileContentNext;
        } catch (IOException e) {
            e.printStackTrace();
            int destNew = findNextIndex(destIndex);
           
            String fileContentNext = queryNode(fileName, destNew);
            return fileContentNext;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        
        String[] splitString = inputMessage.split("#");


        return splitString[2];
    }

    String querySuccessor(String fileName) {

        String key = fileName;
        String keyHash = null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int flag = 0, destIndex = 0;
        for (int i = 0; i < 5; i++) {


            if (keyHash.compareTo(hashPorts.get(i)) <= 0) {
                flag = 1;
                destIndex = i;
                break;
            }

        }
      
        if ((destIndex == myIndex) && (recoveryFlag ==0 ))

        {
            int destNew = findNextIndex(destIndex);
           
            String fileContentNext = queryNode(fileName, destNew);
            return fileContentNext;
        } else {
            String contentFile = queryNode(fileName, destIndex);

            return contentFile;
        }
        
    }

    void deleteSuccessor(String fileName) {

        String key = fileName;
        String keyHash = null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int flag = 0, destIndex = 0;
        for (int i = 0; i < 5; i++) {


            if (keyHash.compareTo(hashPorts.get(i)) <= 0) {
                flag = 1;
                destIndex = i;
                break;
            }

        }

        deleteNode(fileName, destIndex);
       
    }

    private void deleteNode(String fileName, int destIndex) {


        
        messageClass msg = new messageClass();
        msg.type = "deleteMessage";
        msg.sourcePort = myPort;
    
        msg.key = fileName;
        String destId = idPortMap.get(hashPorts.get(destIndex));


        msg.destinationPort = Integer.toString((Integer.parseInt(destId) * 2));

        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(msg.destinationPort));
            socket.setSoTimeout(1700);
        } catch (IOException e) {
            e.printStackTrace();

        }
  
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */

        //used http://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                    
        ObjectOutputStream outputStream = null;
        try {
        
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
            outputStream = new ObjectOutputStream(bufferedOutputStream);
         

        } catch (IOException e) {
            e.printStackTrace();

        }

     
        String msgToSend = msg.type + "#" + msg.key + "#" + msg.value + "#" + msg.sourcePort + "#" + msg.destinationPort + "#" + msg.nextNodePort + "#" + msg.previousNodePort + "#" + msg.sourceID + "#" + msg.replicaID;
    

        System.out.println("deletenode Object to be written = " + msg);
     
        try {
            outputStream.writeObject(msgToSend);
            
            send_ctr++;
        } catch (IOException e) {
            e.printStackTrace();

        }
        try {
            outputStream.flush();
        
        } catch (IOException e) {
            e.printStackTrace();
        }
       

        //http://www.coderpanda.com/java-socket-programming-transferring-of-java-objects-through-sockets/
//socket.
        ObjectInputStream inStream = null;
        try {

           
            BufferedInputStream bufferedInputStream=new BufferedInputStream(socket.getInputStream());
            inStream = new ObjectInputStream(bufferedInputStream);
            
        } catch (IOException e) {

            e.printStackTrace();

        }


        String inputMessage = null;
        try {
            if (inStream != null) {
                inputMessage = (String) inStream.readObject();
                
            }
          
        } catch (IOException e) {
            e.printStackTrace();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        

    }


    void processStar() {


        messageClass starMessage = new messageClass();
        starMessage.type = "starMessage";
        starMessage.sourcePort = myPort;
      
        Socket socket = null;
        for (int i = 11108; i <= 11124; i = i + 4) {
            starMessage.destinationPort = Integer.toString(i);
            if (starMessage.destinationPort.equals(myPort))
                continue;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(starMessage.destinationPort));
                socket.setSoTimeout(1000);
            } catch (IOException e) {
                e.printStackTrace();
            }
          
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */

            //used http://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                
            sendMessageSocket(socket, starMessage);


            //http://www.coderpanda.com/java-socket-programming-transferring-of-java-objects-through-sockets/

            ObjectInputStream inStream = null;
            try {
              
                BufferedInputStream bufferedInputStream=new BufferedInputStream(socket.getInputStream());
                inStream = new ObjectInputStream(bufferedInputStream);
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }


            String inputMessage = null;
            try {
                inputMessage = (String) inStream.readObject();
                

                String[] valuesInput = inputMessage.split("#");
            
                if (valuesInput[2] == null)
                    continue;
                String[] data = valuesInput[2].split(":");
            
                for (int j = 0; j < data.length; j++) {
                    String[] pairs = data[j].split("---");
                    String[] splitString = pairs[0].split("&");
                    String fileName = splitString[3];
                    if (!starFileNameList.contains(fileName)) {
                        starFileNameList.add(fileName);
                        starFileContentList.add((pairs[1]));
                    }
                    
                }

            } catch (IOException e) {
                e.printStackTrace();
                continue;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                continue;
            }
        }
    }

    private void sendMessageSocket(Socket socket, messageClass starMessage) {


       
        ObjectOutputStream outputStream = null;

        try {
        
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
            outputStream = new ObjectOutputStream(bufferedOutputStream);
         
        } catch (IOException e) {
            e.printStackTrace();
           
        }

        
        String msgToSend = starMessage.type + "#" + starMessage.key + "#" + starMessage.value + "#" + starMessage.sourcePort + "#" + starMessage.destinationPort + "#" + starMessage.nextNodePort + "#" + starMessage.previousNodePort + "#" + starMessage.sourceID + "#" + starMessage.replicaID;
       
        System.out.println("Object to be written = " + starMessage);
        

        try {
            outputStream.writeObject(msgToSend);
           
            send_ctr++;
        } catch (IOException e) {
            e.printStackTrace();
           
        }
        try {
            outputStream.flush();
        
        } catch (IOException e) {
            e.printStackTrace();
           

        }
       

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub



        ArrayList<String> fileNameList = new ArrayList<String>();
        ArrayList<String> fileContentList = new ArrayList<String>();
       

        int recoverQuery = 0, queryall = 0;
        //used developer.android.com/guide/topics/providers/content-provider-basics.html
        if (selection.equals("\"@\"") || (selection.equals("\"*\""))) {

            queryall = 1;
            try {
               
                Thread.sleep(180);
                
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            while (recoveryFlag == 0) {

           
                recoverQuery = 1;
            }
            fileNameList = getAllFiles();

        } else
            fileNameList.add(selection);
        
        for (String fileName : fileNameList) {
            File f = new File(fileName);

            String fileContent = null;
           
            String readFileName = checkFile(fileName);
            

            readFileName = checkFile(fileName);
            if (readFileName == null) fileContent = querySuccessor(fileName);
            else {

                fileContent = readFile(readFileName);
                while (fileContent.isEmpty())

                {
                    fileContent = readFile(readFileName);
                }

            }
            fileContentList.add(fileContent);
        }
        MatrixCursor matrixCursor = null;
        if (selection.equals("\"*\"")) {
            starFileNameList = new ArrayList<String>();
            starFileContentList = new ArrayList<String>();
            for (int k = 0; k < fileNameList.size(); k++) {
                if (fileNameList.get(k).contains("&")) {
                    String[] values = fileNameList.get(k).split("&");
                    starFileNameList.add(values[3]);
                } else
                    starFileNameList.add(fileNameList.get(k));
            }
            //starFileNameList.addAll(fileNameList);
            starFileContentList.addAll(fileContentList);

            processStar();
            matrixCursor = createMatrixCursor(starFileNameList, starFileContentList);

        } else

       
            matrixCursor = createMatrixCursor(fileNameList, fileContentList);


        
        return matrixCursor;
        //   return null;
    }


    void queryMe(String selection, String destinationPort, Socket sendSocket) {


        
        ArrayList<String> fileNameList = new ArrayList<String>();
        ArrayList<String> fileContentList = new ArrayList<String>();
        //used developer.android.com/guide/topics/providers/content-provider-basics.html
        if (selection.equals("\"@\"") || (selection.equals("\"*\""))) {

            while (recoveryFlag == 0) {

               
            }
            fileNameList = getAllFiles();

        } else
            fileNameList.add(selection);
        for (String fileName : fileNameList) {
            File f = new File(fileName);

            String fileContent = null;
            String readFile = checkFile(fileName);
            if (readFile == null) {
                fileContent = querySuccessor(fileName);
            } else
                fileContent = readFile(readFile);
            
            fileContentList.add(fileContent);
        }


        ObjectOutputStream outputStream = null;
        try {
      
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(sendSocket.getOutputStream());
            outputStream = new ObjectOutputStream(bufferedOutputStream);

        } catch (IOException e) {
            e.printStackTrace();
        }

        messageClass msg = new messageClass();
        msg.value = fileContentList.get(0);
        String msgToSend = msg.type + "#" + msg.key + "#" + msg.value + "#" + msg.sourcePort + "#" + msg.destinationPort + "#" + msg.nextNodePort + "#" + msg.previousNodePort + "#" + msg.sourceID + "#" + msg.replicaID;
     

        System.out.println("Object to be written = " + msg);
        

        try {
            outputStream.writeObject(msgToSend);
            
            send_ctr++;

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            outputStream.flush();
    
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    void deleteMe(String selection, String destinationPort, Socket sendSocket) {


     
        ArrayList<String> fileNameList = new ArrayList<String>();
        ArrayList<String> fileContentList = new ArrayList<String>();
        //used developer.android.com/guide/topics/providers/content-provider-basics.html
        if (selection.equals("\"@\"") || (selection.equals("\"*\""))) {
            fileNameList = getAllFiles();

        } else
            fileNameList.add(selection);
        for (String fileName : fileNameList) {
            

            String fileContent = null;
            String readFile = checkFile(fileName);

            if (readFile != null) {
              

                deleteFile(readFile);
            } else {
                

            }
          
        }


        ObjectOutputStream outputStream = null;
        try {
           
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(sendSocket.getOutputStream());
            outputStream = new ObjectOutputStream(bufferedOutputStream);

        } catch (IOException e) {
            e.printStackTrace();
        }

        messageClass msg = new messageClass();
        msg.value = "deleteack";
        String msgToSend = msg.type + "#" + msg.key + "#" + msg.value + "#" + msg.sourcePort + "#" + msg.destinationPort + "#" + msg.nextNodePort + "#" + msg.previousNodePort + "#" + msg.sourceID + "#" + msg.replicaID;
       

        System.out.println("Object to be written = " + msg);
        

        try {
            outputStream.writeObject(msgToSend);
            
            send_ctr++;

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            outputStream.flush();
           
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
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

        int seq_no = 0;
        int accept_cr = 0;

        @Override

        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

           
            /*used http://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
            http://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html*/
            while (true) {
                
                Socket socket_read = null;
                try {
                    socket_read = serverSocket.accept();
                    //Log.e(TAG, "ServerTask : doInBaclgrpund : After  accept_cr:" + accept_cr);
                } catch (IOException e) {
                    e.printStackTrace();
                }
              


                try {
                    ObjectInputStream inStream = null;
                 
                    BufferedInputStream bufferedInputStream=new BufferedInputStream(socket_read.getInputStream());
                    inStream = new ObjectInputStream(bufferedInputStream);
                    


                    String inputMessage = null;

                    inputMessage = (String) inStream.readObject();

                   
                    processMessageObject processObject = new processMessageObject();
                    processObject.data = inputMessage;
                    processObject.socket = socket_read;
                
                    new processMessageTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, processObject);
                    
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

                accept_cr++;
            }
            
        }


    }

    int findNextIndex(int inputIndex) {

        if (inputIndex + 1 == 5) {
            return 0;
       
        } else {
            return (inputIndex + 1);
        }
    }

    int findpreviousIndex(int inputIndex) {

        if (inputIndex - 1 == -1) {
            return 4;
          
        } else {
            return (inputIndex - 1);
        }
    }

    public void sendReplica(ContentValues values, int repIndex) {


       
        String key = (String) values.get("key");
        String val = (String) values.get("value");
        messageClass replicaMessage = new messageClass();
        messageClass replicaMessage2 = new messageClass();
        replicaMessage.type = "replicaMessage";
        replicaMessage2.type = "replicaMessage";
        int firsReplicaIndex, secondReplicaIndex;
        if (repIndex + 1 == 5) {
            firsReplicaIndex = 0;
            secondReplicaIndex = 1;
        } else if (repIndex + 2 == 5) {
            firsReplicaIndex = repIndex + 1;
            secondReplicaIndex = 0;
        } else {
            firsReplicaIndex = repIndex + 1;
            secondReplicaIndex = repIndex + 2;
        }
        String destId = idPortMap.get(hashPorts.get(firsReplicaIndex));
        //ContentValues
        replicaMessage.destinationPort = Integer.toString((Integer.parseInt(destId) * 2));
        replicaMessage.value = key + "---" + val;
        replicaMessage2.value = key + "---" + val;
        replicaMessage.replicaID = idPortMap.get(hashPorts.get(repIndex));
        replicaMessage2.replicaID = idPortMap.get(hashPorts.get(repIndex));
        messageClass[] Allmessages = new messageClass[1];
        Allmessages[0] = replicaMessage;
      
        if (destId.equals(myID)) {
            
            replicaInsertSelf(replicaMessage.replicaID, key, val);
        } else
            client_send(Allmessages);
        destId = idPortMap.get(hashPorts.get(secondReplicaIndex));
        //ContentValues
        messageClass[] Allmessages2 = new messageClass[1];
        replicaMessage2.destinationPort = Integer.toString((Integer.parseInt(destId) * 2));
        Allmessages2[0] = replicaMessage2;
        
        if (destId.equals(myID)) {


           
            replicaInsertSelf(replicaMessage2.replicaID, key, val);
        } else
            client_send(Allmessages2);
    }

    void replicaInsertSelf(String replicaID, String key, String val) {

        String fileNameInsert = "rep" + "&" + replicaID + "&" + myID + "&" + key;
        
        FileOutputStream fos = null;
        try {


            fos = getContext().openFileOutput(fileNameInsert, getContext().MODE_WORLD_WRITEABLE);
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            fos.write(val.getBytes());
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void client_send(messageClass[] msgs) {
        messageClass msg = msgs[0];

        try {
            
            String remotePort;
            //type#key#value#sourceport#destinationport#nextnode#prevnode#replicaid
            for (int i = 0; i < msgs.length; i++) {
                msg = msgs[i];
                
                String msgToSend = msg.type + "#" + msg.key + "#" + msg.value + "#" + msg.sourcePort + "#" + msg.destinationPort + "#" + msg.nextNodePort + "#" + msg.previousNodePort + "#" + msg.sourceID + "#" + msg.replicaID;

                int port1 = Integer.parseInt(msg.destinationPort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        port1);
                socket.setSoTimeout(1500);
                
                //used http://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                
                ObjectOutputStream outputStream = null;
                
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
                outputStream = new ObjectOutputStream(bufferedOutputStream);

               

                try {
                   
                    outputStream.writeObject(msgToSend);
                    
                    send_ctr++;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    outputStream.flush();
                
                } catch (IOException e) {
                    e.printStackTrace();
                }

             

                ObjectInputStream inStream = null;
                
                BufferedInputStream bs= new BufferedInputStream(socket.getInputStream());
                inStream = new ObjectInputStream(bs);


                String inputMessage = null;
                

                inputMessage = (String) inStream.readObject();

            }
        } catch (SocketTimeoutException e) {
            e.printStackTrace();
          
        } catch (UnknownHostException e) {
            e.printStackTrace();
            
        } catch (IOException e) {
            e.printStackTrace();
        
        } catch (Exception e) {
            e.printStackTrace();
            
        }


    }


    private class ClientTask extends AsyncTask<messageClass, Void, Void> {

        @Override
        protected Void doInBackground(messageClass... msgs) {

            messageClass msg = msgs[0];

            try {
             
                String remotePort;
                //typekeyvalue#sourceport#destinationport#nextnode#prevnode#replicaid
                for (int i = 0; i < msgs.length; i++) {
                    msg = msgs[i];
                    
                    String msgToSend = msg.type + "#" + msg.key + "#" + msg.value + "#" + msg.sourcePort + "#" + msg.destinationPort + "#" + msg.nextNodePort + "#" + msg.previousNodePort + "#" + msg.sourceID + "#" + msg.replicaID;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msg.destinationPort));

                    
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */

                    //used http://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
              

                    ObjectOutputStream outputStream = null;
                    try {
                        
                        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
                        outputStream = new ObjectOutputStream(bufferedOutputStream);
                   
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    

                    try {
                        outputStream.writeObject(msgToSend);
                        
                        send_ctr++;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {
                        outputStream.flush();
               

                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                   
                }
                
            } catch (UnknownHostException e) {
                e.printStackTrace();
               
            } catch (IOException e) {
                e.printStackTrace();
                
            } catch (Exception e) {
                e.printStackTrace();
               
            }


            return null;

        }
    }


    private class processMessageTask extends AsyncTask<processMessageObject, Void, Void> {

        int seq_no = 0;

        @Override

        protected Void doInBackground(processMessageObject... msgs) {
            String inputMessage = msgs[0].data;


           
            String[] splitString = inputMessage.split("#");
            messageClass inputObject = new messageClass();
            inputObject.type = splitString[0];
            inputObject.key = splitString[1];
            inputObject.value = splitString[2];
            inputObject.sourcePort = splitString[3];
            inputObject.destinationPort = splitString[4];
            inputObject.nextNodePort = splitString[5];
            inputObject.previousNodePort = splitString[6];
            inputObject.sourceID = splitString[7];
            inputObject.replicaID = splitString[8];
            


            if (inputObject.type.equals("insertMessage")) {
                socketInsertFlag = 1;
                String[] splitvalues = inputObject.value.split("---");
               

                Uri mUri;

                ContentValues mContentValues = new ContentValues();
                Uri.Builder uriBuilder = new Uri.Builder();

                uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");

                uriBuilder.scheme("content");
                mUri = uriBuilder.build();
                mContentValues.put("key", splitvalues[0]);

                mContentValues.put("value", splitvalues[1]);

                insertByInput(mUri, mContentValues, msgs[0].socket);

            } else if (inputObject.type.equals("replicaMessage")) {
                String[] splitvalues = inputObject.value.split("---");
                


                insertReplica(splitvalues[0], splitvalues[1], inputObject.replicaID, msgs[0].socket);

            } else if (inputObject.type.equals("queryMessage")) {
              

                queryMe(inputObject.key, inputObject.sourcePort, msgs[0].socket);
            } else if (inputObject.type.equals("starMessage")) {
                while (recoveryFlag == 0) {

                   
                }
                starMessageInput(inputObject, msgs[0].socket);

            } else if (inputObject.type.equals("recoveryMessage")) {
                while (recoveryFlag == 0) {

                    
                }
                recoverMessageInput(inputObject, msgs[0].socket);
            } else if (inputObject.type.equals("deleteMessage")) {
                
                while (recoveryFlag == 0) {

                    
                }
                deleteMe(inputObject.key, inputObject.sourcePort, msgs[0].socket);
            }
            
            return null;
        }

       


    }

    private class RecoveryMessageTask extends AsyncTask<processMessageObject, Void, Void> {

        int seq_no = 0;

        @Override

        protected Void doInBackground(processMessageObject... msgs) {


            ArrayList<String> fileNameList = getAllFiles();


            for (String fileName : fileNameList) {
                
                String deleteFileName = checkFile(fileName);
                if (deleteFileName != null)

                    deleteFile(deleteFileName);
                
            }
            
            recoveryMap = new HashMap<String, String>();
            
            int destIndex = findpreviousIndex(myIndex);
            recoveryMessageProcess(destIndex, 0);
            destIndex = findpreviousIndex(destIndex);
            recoveryMessageProcess(destIndex, 0);
            destIndex = findNextIndex(myIndex);
            recoveryMessageProcess(destIndex, 1);
            destIndex = findNextIndex(destIndex);
            
            recoveryMessageProcess(destIndex, 1);
            FileOutputStream fos = null;
            int f = 0;
            Set<String> keysMap = recoveryMap.keySet();
           

            for (String fileNameInsert : keysMap) {
                
                String val = recoveryMap.get(fileNameInsert);
                File file;
                file = new File(String.valueOf(getContext().getFilesDir()));

                int writeFlag = 0;
                File[] files = file.listFiles();

                for (File nameFile : files) {

                    if (!nameFile.isDirectory()) {

                        String databaseFile = nameFile.getName();
                       
                        if (databaseFile.equals(fileNameInsert)) {
                            writeFlag = 1;
                            break;
                        }

                    }
                }
                if (writeFlag == 1)
                    continue;
                try {

                    f++;
                    
                    fos = getContext().openFileOutput(fileNameInsert, getContext().MODE_WORLD_WRITEABLE);

                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                try {
                    

                    fos.write(val.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            
            recoveryFlag = 1;
            return null;
        }

        void recoveryMessageProcess(int destIndex, int myfileflag) {
            
            String destId = idPortMap.get(hashPorts.get(destIndex));
            messageClass msg = new messageClass();
            msg.type = "recoveryMessage";
            msg.sourcePort = myPort;
            
            msg.key = myID;

            msg.destinationPort = Integer.toString((Integer.parseInt(destId) * 2));

            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msg.destinationPort));
                

                //used http://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
                  
                ObjectOutputStream outputStream = null;

            
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
                outputStream = new ObjectOutputStream(bufferedOutputStream);
               
                String msgToSend = msg.type + "#" + msg.key + "#" + msg.value + "#" + msg.sourcePort + "#" + msg.destinationPort + "#" + msg.nextNodePort + "#" + msg.previousNodePort + "#" + msg.sourceID + "#" + msg.replicaID;
                

                System.out.println("Object to be written = " + msg);
                


                outputStream.writeObject(msgToSend);
                
                send_ctr++;


                outputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();

            }
            


            //http://www.coderpanda.com/java-socket-programming-transferring-of-java-objects-through-sockets/

            ObjectInputStream inStream = null;

            try {


                BufferedInputStream bufferedInputStream=new BufferedInputStream(socket.getInputStream());
                inStream = new ObjectInputStream(bufferedInputStream);
               


                String inputMessage = null;
               
                inputMessage = (String) inStream.readObject();
       
                String[] valuesInput = inputMessage.split("#");
                
                if (valuesInput[2] != null) {
                  
                    if (valuesInput[2].compareTo("null") != 0) {
                        String[] data = valuesInput[2].split(":");
                        
                        for (int j = 0; j < data.length; j++) {
                            if (myfileflag == 1) {
                                if (!data[j].contains(myID))
                                    continue;
                            } else {
                                if (!data[j].contains("my&"))
                                    continue;
                            }
                            String[] pairs = data[j].split("---");
                            String[] splitString = pairs[0].split("&");
                            String fileName = splitString[3];
                            int flagAdd = 0;
                           
                            if (flagAdd == 0) {
                                if (myfileflag == 1) {

                                    String fileNameInsert = "my" + "&" + myID + "&" + myID + "&" + fileName;
                                    
                                    recoveryMap.put(fileNameInsert, pairs[1]);

                                    
                                } else {
                                    
                                    String fileNameInsert = "rep" + "&" + splitString[1] + "&" + myID + "&" + fileName;
                                   
                                    recoveryMap.put(fileNameInsert, pairs[1]);
                                    
                                }
                               
                            }
                            
                        }
                    }
                }
                
            } catch (SocketTimeoutException e) {
                e.printStackTrace();

            } catch (IOException e) {
                e.printStackTrace();

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        }


    }
}