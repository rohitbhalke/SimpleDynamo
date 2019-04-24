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
	public static final String LDUMP = "@";
	public static final String GDUMP = "*";
	public static final String GDUMP_QUERY = "GDUMP_QUERY";

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private ContentResolver mContentResolver;
	public static Context currentContext;
	private Uri mUri;


	public final String INSERT = "INSERT";

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
		/*
			Add to proper partition and replicate to 2 successors
		 */
		String key = (String) values.get("key");
		String value = (String) values.get("value");

		String partitionPort = getPartitionPort(key);
		if (partitionPort.equals(myPortId)) {
			// Insert in local file system
			insertInLocalDb(uri, key, value);
		}

		// now create a client task to add the same key in 2 of its successors

		String msg = INSERT;
		Message message = new Message(INSERT);
		message.setKey(key);
		message.setValue(value);
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

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		currentContext = this.getContext();
		mContentResolver = currentContext.getContentResolver();

		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");

		TelephonyManager tel = (TelephonyManager) currentContext.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String portNumber = String.valueOf((Integer.parseInt(portStr) * 2));

		Log.i("BOOT_UP", portNumber);

		myPortId = portStr;         // Ex: 5554
		mySocketId = portNumber;    // Ex: 11108

		initializeDynamoLookupTable();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
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
			return getAllDataFromLocal(uri);
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

							dos.writeUTF("GDUMP_QUERY");
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
					String key = pair.split(" ")[0];
					String value = pair.split(" ")[1];
					if(!map.containsKey(key)) {
						map.put(key, value);
                        String[] columnValues = {key, value};
						matrixCursor.addRow(columnValues);
					}
				}
				return matrixCursor;
		}
		else {
			String key = selection;
			String partitionPort = getPartitionPort(key);
			if(partitionPort.equals(myPortId)) {
				return findInLocal(key);
			}
			else {
				MatrixCursor matrixCursor = new MatrixCursor(columnNames);
				String target = getSocketNumber(partitionPort);
				Socket socket = null;
				try{
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(target));
					OutputStream stream = socket.getOutputStream();

					DataOutputStream dos = new DataOutputStream(stream);

					Message message = new Message(QUERY);
					message.setKey(key);

					dos.writeUTF(message.getString());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				InputStream inputStream = null;
				try {
					inputStream = socket.getInputStream();
					DataInputStream dis = new DataInputStream(inputStream);
					String message = dis.readUTF();

					String value = message;
					String[] columns = {key, value};
					matrixCursor.addRow(columns);
					return matrixCursor;
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}

		return null;
	}

	private Cursor findInLocal(String keyToFind) {

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
			cursor.addRow(columnValues);

			return cursor;
		}
		catch (Exception e) {
			Log.v("Exception", e.getMessage());
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		SimpleDynamoProvider dynamo;

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
						dynamo.insertInLocalDb(mUri, key, value);
					}
					if(messageType.equals(GDUMP_QUERY)) {
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
						dos.writeUTF(sb.toString());
						dos.flush();
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
						String target = getSocketNumber(partitionPort);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(target));

						stream = socket.getOutputStream();

						dos = new DataOutputStream(stream);

						dos.writeUTF(message);
					}

					String[] successors = getSuccessors(partitionPort);
					for (String successor : successors) {
						// Send message to successors to insert the value
						String target = getSocketNumber(successor);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(target));

						stream = socket.getOutputStream();

						dos = new DataOutputStream(stream);

						dos.writeUTF(message);
					}
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


}