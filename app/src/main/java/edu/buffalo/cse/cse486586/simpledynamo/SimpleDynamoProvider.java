package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.TreeMap;

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

public class SimpleDynamoProvider extends ContentProvider {

	static String[] emulatorPorts = {"5554", "5556", "5558", "5560", "5562"};
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	TreeMap<String, String> dynamoLookupTable = new TreeMap<String, String>(); // Sorted Hash value of Port - Port
	public static ArrayList<String> portNumbers = new ArrayList<String>();    // Sorted port numbers
	public static ArrayList<String> hashValues = new ArrayList<String>();    // Sorted hash values
	public static String myPortId = "";
	public static String mySocketId = "";

	public static final String QUERY = "QUERY";
	public static final String DELETE = "DELETE";
	public static final String LDUMP = "@";
	public static final String GDUMP = "*";
	public static final String GDUMP_QUERY = "GDUMP_QUERY";
	public static final String GDUMP_DELETE = "GDUMP_QUERY";

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private ContentResolver mContentResolver;
	public static Context currentContext;
	private Uri mUri;


	public static HashMap<String, String> queue = new HashMap<String, String>();	// Hold the messages which are being sent


	public final String INSERT = "INSERT";

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		String[] columnNames = {"key", "value"};
		if(selection.equals(LDUMP)) {
			deleteAllDataFromLocal(uri);
		}
		else if(selection.equals(GDUMP)) {
			// Send message to everyone to delete their content
			deleteAllDataFromLocal(uri);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, GDUMP_DELETE);
		}
		else {
			String key = selection;
			String partitionPort = getPartitionPort(key);
			Log.i("Delete", key+" :: " + partitionPort);
			if(partitionPort.equals(myPortId)) {
				deleteFileFromLocal(uri, selection);
			}
			String msg = DELETE;
			Message message = new Message(DELETE);
			message.setKey(key);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, message.getString(), partitionPort);
		}

		return 0;
	}

	private void deleteAllDataFromLocal(Uri uri) {
		File fileDirectory = currentContext.getFilesDir();
		File[] listOfFiles = fileDirectory.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			File currentFile = listOfFiles[i];
			currentFile.delete();
		}
	}

	private void deleteFileFromLocal(Uri uri, String key) {
		File fileDirectory = currentContext.getFilesDir();
		File[] listOfFiles = fileDirectory.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			File currentFile = listOfFiles[i];
			if(currentFile.getName().equals(key)) {
				currentFile.delete();
				break;
			}
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		/*
			Add to proper partition and replicate to 2 successors
		 */
		String key = (String) values.get("key");
		String value = (String) values.get("value");

		String partitionPort = getPartitionPort(key);
		Log.i("Insert_partition_for",key+ ":" + partitionPort+":"+myPortId);

		String[] successors = getSuccessors(partitionPort);

		if (partitionPort.equals(myPortId) || successors[0].equals(myPortId) || successors[1].equals(myPortId)) {
			// Insert in local file system if the key's coordinator node is this node OR
			// key's successor node is this node
			insertInLocalDb(uri, key, value);
		}

		// now create a client task to add the same key in 2 of its successors

		String msg = INSERT;
		Message message = new Message(INSERT);
		message.setKey(key);
		message.setValue(value);

		// REMEMBER
		queue.put(key, value);
		Log.i("QUEUE_SIZE_BEFORE", String.valueOf(queue.size()));
		// REMEMBER
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, message.getString(), partitionPort);


		return null;
	}


	public Uri insertInLocalDb(Uri uri, String key, String value) {
		String filename = key;
		String fileContents = value;
		FileOutputStream outputStream;

		String fileHashValue = "";
		try {
			fileHashValue = genHash(filename);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		try {
			Log.i("CREATE_FILE", filename + "   " + fileContents + "  sha: " + fileHashValue);
			// Versioning logic
			if(fileExists(filename)) {
				String existingValue = findKeyInLocal(filename);
				int existingVersion = Character.getNumericValue(existingValue.charAt(1));
				int newVersion = existingVersion + 1;
				fileContents = "V"+String.valueOf(newVersion)+"_"+fileContents;
				//fileContents = "V1_"+fileContents;
			}
			else {
				fileContents = "V0_"+fileContents;
			}
			outputStream = currentContext.openFileOutput(filename, Context.MODE_PRIVATE);
			synchronized (outputStream) {
				outputStream.write(fileContents.getBytes());
				outputStream.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		Log.v("insert", key + " : " + value);
		return uri;
	}

	public void upgradeTheVersionNumber(String key) {
		try {
			FileInputStream fileReaderStream = currentContext.openFileInput(key);
			InputStreamReader inputStream = new InputStreamReader(fileReaderStream);
			BufferedReader br = new BufferedReader(inputStream);
			String messageReceived = br.readLine();
			Log.v("upgradeTheVersionNumber", messageReceived);

			// update the version number
			String value = messageReceived.substring(3);
			int versionNum = Character.getNumericValue(messageReceived.charAt(1));
			versionNum++;
			String newVal = "V"+String.valueOf(versionNum)+"_"+value;
			Log.v("NewValue", newVal);
			FileOutputStream outputStream;
			outputStream = currentContext.openFileOutput(key, Context.MODE_PRIVATE);
			synchronized (outputStream) {
				outputStream.write(newVal.getBytes());
				outputStream.close();
			}
		}
		catch (Exception e) {
			Log.v("Exception", e.getMessage());
		}

	}

	public boolean fileExists(String filename) {
		File fileDirectory = currentContext.getFilesDir();
		File[] listOfFiles = fileDirectory.listFiles();

		for(int i=0;i<listOfFiles.length;i++) {
			if(listOfFiles[i].getName().equals(filename)) {
				return true;
			}
		}

		return false;

	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		currentContext = this.getContext();
		mContentResolver = currentContext.getContentResolver();

		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");

		TelephonyManager tel = (TelephonyManager) currentContext.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String portNumber = String.valueOf((Integer.parseInt(portStr) * 2));

		Log.i("BOOT_UP", portStr+":"+portNumber);

		myPortId = portStr;         // Ex: 5554
		mySocketId = portNumber;    // Ex: 11108

		initializeDynamoLookupTable();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			e.printStackTrace();
		}

		// Differentiate RECOVER and BOOTUP

		File fileDirectory = currentContext.getFilesDir();
		File[] listOfFiles = fileDirectory.listFiles();

		if(listOfFiles.length > 0) {
			Log.i("REINCARNATE", "Calling REINCARNATE");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REBORN");
			/*
					Cant call reIncarnate() from here as
					https://stackoverflow.com/questions/6343166/how-do-i-fix-android-os-networkonmainthreadexception
			 */

			//reIncarnate();
		}

		return false;
	}

	public void initializeDynamoLookupTable() {
		for (String port : emulatorPorts) {
			try {
				dynamoLookupTable.put(genHash(port), port);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		for (String hashValue : dynamoLookupTable.keySet()) {
			hashValues.add(hashValue);
		}

		for (String port : dynamoLookupTable.values()) {
			portNumbers.add(port);
		}

	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		String[] columnNames = {"key", "value"};
		if(selection.equals(LDUMP)) {
			// No need to Implement versioning part here
			MatrixCursor matrixCursor = getAllDataFromLocal(uri);
			return removeVersioningInfoAndReturn(matrixCursor);
		}
		else if(selection.equals(GDUMP)) {
			// Send messages to each avd and combine their result and return
			DataInputStream dis = null;
			StringBuilder result = new StringBuilder();
				for(String port : emulatorPorts) {
					Socket socket = null;
					if (!port.equals(myPortId)) {
						try {
							String target = getSocketNumber(port);
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(target));
							OutputStream stream = socket.getOutputStream();

							DataOutputStream dos = new DataOutputStream(stream);

							Message message = new Message(GDUMP_QUERY);

							dos.writeUTF(message.getString());
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}

						InputStream inputStream = null;
						try {
							inputStream = socket.getInputStream();
							dis = new DataInputStream(inputStream);
							String message = dis.readUTF();

							result.append(message);

						} catch (IOException e) {
							e.printStackTrace();
						}

					}
				}

				// Now fetch local data
			MatrixCursor matrixCursor = getAllDataFromLocal(uri);

				String[] output = result.toString().split(",");
				HashMap<String, String> map = new HashMap<String, String>();
				for(String pair: output) {
					Log.i("pair", pair);
					String key = pair.split(" ")[0];
					String value = pair.split(" ")[1];
					if(!map.containsKey(key)) {
						map.put(key, value);
					}
					else {
						String existingValue = map.get(key);
						if((int)existingValue.charAt(1) < (int)value.charAt(1)) {
							map.put(key, value);
						}
					}
				}

				// Now remove the versioning part and return matrixcursor

				for(String key:map.keySet()) {
					String[] columnValues = {key, map.get(key).substring(3)};
					matrixCursor.addRow(columnValues);
				}
				return matrixCursor;
		}
		else {
			String key = selection;
			String partitionPort = getPartitionPort(key);
			Log.i("Query", key+" :: " + partitionPort);


			String[] successors = getSuccessors(partitionPort);
			String[] keyContainerPorts = {partitionPort, successors[0], successors[1]};

			ArrayList<String> values = new ArrayList<String>();

			// Query all 3 nodes and then choose the biggest versioning number and return


			MatrixCursor matrixCursor = new MatrixCursor(columnNames);

			if(keyContainerPorts[0].equals(myPortId) || keyContainerPorts[1].equals(myPortId) || keyContainerPorts[2].equals(myPortId)) {
				Log.i("Gettin_Value_From", key +"   "+ myPortId);
				String result = findKeyInLocal(key);
				if(result==null){
					Log.i("No_File_In_Local_For", key);
				}
				if(result!=null) {
					values.add(result);
				}
			}
				// Ask 3 nodes about the data
			Log.i("INFO", key+" " + keyContainerPorts.length);
			for(String port : keyContainerPorts) {
				if (!port.equals(myPortId)) {
					Log.i("Gettin_Value_From_in", key +"   "+ port);
					String target = getSocketNumber(port);
					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(target));
						OutputStream stream = socket.getOutputStream();

						DataOutputStream dos = new DataOutputStream(stream);

						Message message = new Message(QUERY);
						message.setKey(key);

						dos.writeUTF(message.getString());
					} catch (UnknownHostException e) {
						Log.e("UnknownHost_Send", "UnknownHost Exception::" + target);
						e.printStackTrace();
					} catch (IOException e) {
						Log.e("IOException_send", "UnknownHost Exception::" + target);
						e.printStackTrace();
					} catch (Exception e) {
						Log.e("Query_Exception_Send_DS", "For target::" + target);
						// Ask one of its successor if this is failing
						// Here need to query the replicas of the target node, and return their values

						// get the successor of the target node here

					}
					InputStream inputStream = null;
					try {
						inputStream = socket.getInputStream();
						DataInputStream dis = new DataInputStream(inputStream);
						String message = dis.readUTF();

						String value = message;
						if(value.length()>0) {
                            values.add(value);
                        }
                        Log.i("GOT_VALUE_FOR_FROM_V", key +":"+port+value);
						//String[] columns = {key, value};
						//matrixCursor.addRow(columns);
						//return matrixCursor;

					} catch (IOException e) {
						Log.i("IOException_Query_Read", "IOException_Query_Read");
						//e.printStackTrace();
					} catch (Exception e) {
						// Ask one of its successor if this is failing
						// Here need to query the replicas of the target node, and return their values

						// get the successor of the target node here
						Log.e("Query_Except_Receive_DS", "For target::" + target);

						try {
							if (socket != null) {
								socket.close();
							}
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
				}

			}
			String latestValue = findLatestValue(values);
			//Remember QUEUE condition here

			if(queue.containsKey(key)){
				Log.i("PLAIN_VALUE", latestValue);
				latestValue = queue.get(key);
				Log.i("QUEUE_VALUE", latestValue);
			}

			//Remember QUEUE condition here
			String[] columnValues = {key, latestValue};
			matrixCursor.addRow(columnValues);
			Log.i("Return_Key_Value", key +"  " + latestValue);
			return matrixCursor;
		}
		//return null;
	}

	private Cursor removeVersioningInfoAndReturn(MatrixCursor matrixCursor) {
		String[] columnNames = {"key", "value"};
		MatrixCursor result = new MatrixCursor(columnNames);
		int keyIndex = matrixCursor.getColumnIndex(KEY_FIELD);
		int valueIndex = matrixCursor.getColumnIndex(VALUE_FIELD);
		while(matrixCursor.moveToNext()) {
			String key = matrixCursor.getString(keyIndex);
			String value = matrixCursor.getString(valueIndex);
			String[] columnValues = {key, value.substring(3)};
			result.addRow(columnValues);
		}
		return result;
	}


	private String findLatestValue(ArrayList<String> values) {
		Log.i("Exception_Preq", String.valueOf(values.size()));
		for(int i=0;i<values.size();i++) {
			Log.i("Exception_Here", values.get(i));
		}
		int version = (int) values.get(0).charAt(1);
		String latest = values.get(0).substring(3);
		Log.i("FindLatestValue", latest);
		for(int i=1;i<values.size();i++) {
			int local_version = (int) values.get(i).charAt(1);
			if(local_version>version) {
				version = local_version;
				latest = values.get(i).substring(3);
			}
			Log.i("FindLatestValue", latest);
		}
		return latest;
	}

	private String QueryReplica(String partitionPort, String key) {

		String[] successors = getSuccessors(partitionPort);

		String target = getSocketNumber(successors[0]);
		Socket socket=null;
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(target));
			OutputStream stream = socket.getOutputStream();

			DataOutputStream dos = new DataOutputStream(stream);

			Message message = new Message(QUERY);
			message.setKey(key);

			dos.writeUTF(message.getString());

			InputStream inputStream = socket.getInputStream();
			DataInputStream dis = new DataInputStream(inputStream);
			String value = dis.readUTF();
			dos.close();
			dis.close();
			stream.close();
			return value;
		}
		catch (Exception e) {
			Log.e("Query_Replica_Excep", successors[0]);
		}
		finally {
			try {
				if (socket != null)
					socket.close();
			} catch (IOException e) {
				Log.e(TAG, "Error while disconnecting socket");
			}
		}
		return null;
	}


	private String findKeyInLocal(String keyToFind) {

		try{
			Log.v("query", keyToFind);
			FileInputStream fileReaderStream = currentContext.openFileInput(keyToFind);
			InputStreamReader inputStream = new InputStreamReader(fileReaderStream);
			BufferedReader br = new BufferedReader(inputStream);
			String messageReceived = br.readLine();
			Log.v("File Content: ", messageReceived);
			String[] columnNames = {"key", "value"};
			MatrixCursor cursor = new MatrixCursor(columnNames);
			String[] columnValues = {keyToFind, messageReceived};
			return messageReceived;
		}
		catch (Exception e) {
			Log.v("Exception", e.getMessage());
		}
		return null;
	}

	private Cursor findInLocal(String keyToFind) {

		try{
			Log.v("query_findInLocal", keyToFind);
			FileInputStream fileReaderStream = currentContext.openFileInput(keyToFind);
			InputStreamReader inputStream = new InputStreamReader(fileReaderStream);
			BufferedReader br = new BufferedReader(inputStream);
			String messageReceived = br.readLine();
			Log.v("File Content: ", messageReceived);
			String[] columnNames = {"key", "value"};
			MatrixCursor cursor = new MatrixCursor(columnNames);
			String[] columnValues = {keyToFind, messageReceived};
			cursor.addRow(columnValues);

			return cursor;
		}
		catch (Exception e) {
			Log.v("Exception", e.getMessage());
			//return null;
		}
		return null;
	}


	private MatrixCursor getAllDataFromLocal(Uri uri) {
		File fileDirectory = currentContext.getFilesDir();
		File[] listOfFiles = fileDirectory.listFiles();
		String[] columnNames = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(columnNames);
		try {
			for (int i = 0; i < listOfFiles.length; i++) {
				File currentFile = listOfFiles[i];
				FileInputStream fileReaderStream = currentContext.openFileInput(currentFile.getName());
				InputStreamReader inputStream = new InputStreamReader(fileReaderStream);
				BufferedReader br = new BufferedReader(inputStream);
				String messageReceived = br.readLine();
				Log.v("File Content: ", messageReceived);
				String[] columnValues = {currentFile.getName(), messageReceived};
				cursor.addRow(columnValues);
			}
		}
		catch (Exception e){
			Log.e("Exception", "Exception in reading all local files");
			System.out.println(e.getStackTrace());
		}
		return cursor;
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

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	// return thes port where the key should reside
	private String getPartitionPort(String key) {
		String hashOfKey = "";
		int portIndex = -1;
		try {
			hashOfKey = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < hashValues.size(); i++) {
			if (hashValues.get(i).compareTo(hashOfKey) > 0) {
				portIndex = i;
				break;
			}
		}
		if (portIndex == -1) {
			portIndex = 0;
		}

		return portNumbers.get(portIndex);    // return port like 5554
	}


	private void reIncarnate() {

		File fileDirectory = currentContext.getFilesDir();
		File[] listOfFiles = fileDirectory.listFiles();

		// Delete everything first
		for(File file : listOfFiles) {
			file.delete();
		}

		// get messages from 2 successors
		String[] successors = getSuccessors(myPortId);

		// get messages from 2 predessors
		String[] predessors = getPredessors(myPortId);

		String[] nodes = {successors[0],successors[1], predessors[0], predessors[1]};

		// Take messages from these 4 nodes first
		StringBuilder result = new StringBuilder();


		DataInputStream dis = null;
		for(String port : nodes) {
			Socket socket = null;
				try {
					String target = getSocketNumber(port);
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(target));
					OutputStream stream = socket.getOutputStream();

					DataOutputStream dos = new DataOutputStream(stream);

					Message message = new Message(GDUMP_QUERY);

					dos.writeUTF(message.getString());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

				InputStream inputStream = null;
				try {
					inputStream = socket.getInputStream();
					dis = new DataInputStream(inputStream);
					String message = dis.readUTF();

					result.append(message);
					Log.i("R-got-data-from", port);
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}

		/*
				Take all these messages and filter them and maintain a hashtable
		 */

		String[] output = result.toString().split(",");
		HashMap<String, String> map = new HashMap<String, String>();
		for(String pair: output) {
			Log.i("pair", pair);
			String key = pair.split(" ")[0];
			String value = pair.split(" ")[1];
			if(!map.containsKey(key)) {
				map.put(key, value);
			}
			else {
				if((int)map.get(key).charAt(1) < (int)value.charAt(1)) {
					map.put(key, value);
				}
			}
		}

		// Now get all the messages without the version number values

		for(String key:map.keySet()) {
			String new_val = map.get(key).substring(3);
			map.put(key, new_val);
		}

		/*
		Now this node got all the messages from 4 nodes, now need to decide
		 which messages belong to this node
			1) Messages which are meant for this node ie this node is the coordinator node
			2) Messages of two predessor nodes

			*/

		for(String key : map.keySet()) {
			String partitionNode = getPartitionPort(key);
			Log.i("KEY_Partition_Port_1_2", key+":"+partitionNode+":"+predessors[0]+":"+predessors[1]+":"+myPortId);
			if(partitionNode.equals(myPortId) || partitionNode.equals(predessors[0]) ||
			partitionNode.equals(predessors[1])) {

				/*
						Here, while this node is synching with other nodes,
						there could come a insert req on this node with same key and diff val
						So this could be a problem, as while synching this node update the newly inserted value
						So handle it here

				 */
//				if(!fileExists(key)) {
//					insertInLocalDb(mUri, key, map.get(key));
//				}

				if(fileExists(key)) {
					// Don't update the value instead update the version number
					upgradeTheVersionNumber(key);
				}
				else {
					insertInLocalDb(mUri, key, map.get(key));
				}
			}
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		SimpleDynamoProvider dynamo = new SimpleDynamoProvider();

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			/*
			 * TODO: Fill in your server code that receives messages and passes them
			 * to onProgressUpdate().
			 */
			Socket socket = null;
			DataInputStream dis = null;
			InputStream stream = null;


			while (true) {
				try {
					socket = serverSocket.accept();
					stream = socket.getInputStream();
					dis = new DataInputStream(stream);
					String message = dis.readUTF();

					String[] splittedMessage = message.split(";");
					String messageType = splittedMessage[0].split(":")[1];

					if(messageType.equals(INSERT)) {
						String key = splittedMessage[1].split(":")[1];
						String value = splittedMessage[2].split(":")[1];
						Log.i("Insert_Server", key+"  :  " +value);
						dynamo.insertInLocalDb(mUri, key, value);
					}
					else if(messageType.equals(GDUMP_QUERY)) {
						MatrixCursor matrixCursor = dynamo.getAllDataFromLocal(mUri);
						StringBuilder sb = new StringBuilder();

						int keyIndex = matrixCursor.getColumnIndex(KEY_FIELD);
						int valueIndex = matrixCursor.getColumnIndex(VALUE_FIELD);

						while(matrixCursor.moveToNext()) {
							sb.append(matrixCursor.getString(keyIndex));
							sb.append(" ");
							sb.append(matrixCursor.getString(valueIndex));
							sb.append(",");
							// key value,key value,key value
						}
						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						Log.i("GDUMP_QUERY_ANSWER", sb.toString());
						dos.writeUTF(sb.toString());
						dos.flush();
					}
					else if(messageType.equals(QUERY)) {
						String key = splittedMessage[1].split(":")[1];
						Cursor cursor = dynamo.findInLocal(key);
						String value = "";
						if(cursor != null) {
                            while (cursor.moveToNext()) {
                                value = cursor.getString(cursor.getColumnIndex(VALUE_FIELD));
                            }
                        }
                        if(cursor==null){
                            value = "";
                        }
						Log.i("Query_Search",key + "   :  "+ value);
						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						dos.writeUTF(value);
						dos.flush();
					}
					else if(messageType.equals(DELETE)) {
						String key = splittedMessage[1].split(":")[1];
						dynamo.deleteFileFromLocal(mUri, key);
					}
					else if(messageType.equals(GDUMP_DELETE)) {
						dynamo.deleteAllDataFromLocal(mUri);
					}
				} catch (IOException e) {
					Log.i("Server_Failed", "YE");
					Log.e(TAG, "Client Disconnected");
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
					Log.e(TAG, "Failed to accept connection");
				} finally {
					try {
						if (socket != null)
							socket.close();

					} catch (IOException e) {
						Log.e(TAG, "Error while disconnecting socket");
					}
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
			String order = msgs[0];

			Socket socket = null;
			OutputStream stream = null;
			DataOutputStream dos = null;
			DataInputStream dis = null;

			try {
				if (order.equals(INSERT)) {
					String message = msgs[1];
					String partitionPort = msgs[2];
					if (!partitionPort.equals(myPortId)) {
						// Send message to that node to insert value
						Log.i("PARTITION_PORT_For", message +":"+ partitionPort);
						String target = getSocketNumber(partitionPort);
						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(target));

							stream = socket.getOutputStream();

							dos = new DataOutputStream(stream);

							dos.writeUTF(message);

							socket.close();
						}
						catch (IOException err) {
							Log.e("EXCEPTION_WRITE", "Exception while sending key to its partition node");
						}
					}

					String[] successors = getSuccessors(partitionPort);

					for (String successor : successors) {
						// Send message to successors to insert the value
						if(!successor.equals(myPortId)) {	// This is already handled
							Log.i("REPLICATION_For", message+":"+successor);
							String target = getSocketNumber(successor);
							try {
								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(target));

								stream = socket.getOutputStream();

								dos = new DataOutputStream(stream);

								dos.writeUTF(message);
								socket.close();
							}
							catch (IOException err) {
								Log.e("EXCEPTION_Replica_WRITE", "Exception while sending key to its REPLICA node");
								socket.close();
							}
						}
					}
					String key = message.split(";")[1].split(":")[1];
					queue.remove(key);
					Log.i("QUEUE_SIZE_AFTER", String.valueOf(queue.size()));
				}
				else if(order.equals(GDUMP_DELETE)) {
					for(String port : emulatorPorts) {
						if (!port.equals(myPortId)) {
							try {
								String target = getSocketNumber(port);
								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(target));
								stream = socket.getOutputStream();

								dos = new DataOutputStream(stream);

								Message message = new Message(GDUMP_DELETE);

								dos.writeUTF(message.getString());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
				else if(order.equals(DELETE)) {
					String message = msgs[1];
					String partitionPort = msgs[2];
					if (!partitionPort.equals(myPortId)) {
						// Send message to that node to Delete value
						String target = getSocketNumber(partitionPort);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(target));

						stream = socket.getOutputStream();

						dos = new DataOutputStream(stream);

						dos.writeUTF(message);
					}

					String[] successors = getSuccessors(partitionPort);
					for (String successor : successors) {
						// Send message to successors to delete the value

						String target = getSocketNumber(successor);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(target));

						stream = socket.getOutputStream();

						dos = new DataOutputStream(stream);

						dos.writeUTF(message);
					}
				}
				else if(order.equals("REBORN")) {
					reIncarnate();
				}
			} catch (UnknownHostException unknownHost) {
				Log.i("Unknown_host", "Unknown_host");
			} catch (IOException io) {
				System.out.println(io.getStackTrace());
			} finally {
				try {
					if (stream != null)
						stream.close();
					if (dis != null)
						dis.close();
					if (dos != null)
						dos.close();
					if (socket != null)
						socket.close();
				} catch (IOException e) {
					Log.e(TAG, "Error while disconnecting socket");
				}
			}
			return null;
		}
	}


	private String getSocketNumber(String port) {
		return String.valueOf(Integer.parseInt(port) * 2);
	}

	/*
			Method to return the 2 successors of Port
	 */
	private String[] getSuccessors(String port) {
		int portIndex = -1;
		String[] successors = new String[2];

		String portsT = "";
		for (int i = 0; i < portNumbers.size(); i++) {
			portsT +=portNumbers.get(i) +" ";
		}
		Log.i("PORTS", portsT);
		for (int i = 0; i < portNumbers.size(); i++) {
			if (portNumbers.get(i).equals(port)) {
				portIndex = i;
				break;
			}
		}
		if (portIndex != -1) {
			successors[0] = portNumbers.get((portIndex + 1) % portNumbers.size());
			successors[1] = portNumbers.get((portIndex + 2) % portNumbers.size());
		}
		return successors;
	}

		/*
                Method to return the 2 predessors of Port
         */
	private String[] getPredessors(String port) {
		int portIndex = -1;
		String[] predessors = new String[2];
		for (int i = 0; i < portNumbers.size(); i++) {
			if (portNumbers.get(i).equals(port)) {
				portIndex = i;
				break;
			}
		}


		if (portIndex != -1) {
			//if(portIndex < 2) {
				portIndex += portNumbers.size();
			//}
			predessors[0] = portNumbers.get((portIndex - 1) % portNumbers.size());
			predessors[1] = portNumbers.get((portIndex - 2) % portNumbers.size());
		}
		return predessors;
	}


}