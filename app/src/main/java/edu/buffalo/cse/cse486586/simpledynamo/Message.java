package edu.buffalo.cse.cse486586.simpledynamo;

public class Message {

    private String messageType = null;
    private String client =null;
    private String uri =null;
    private String queryPort = null;

    public String getCurrentPort() {
        return currentPort;
    }

    public void setCurrentPort(String currentPort) {
        this.currentPort = currentPort;
    }

    private String currentPort = null;
    private String keyToSearch = null, associatedPort=null;

    private String GDUMP_Response = null;
    public String getGDUMP_Response() {
        return GDUMP_Response;
    }

    public void setGDUMP_Response(String GDUMP_Response) {
        this.GDUMP_Response = GDUMP_Response;
    }


    String key = null, value= null;
    String queryAnswer = null;


    public String getQueryAnswer() {
        return queryAnswer;
    }

    public void setQueryAnswer(String queryAnswer) {
        this.queryAnswer = queryAnswer;
    }



    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }



    public String getQueryPort() {
        return queryPort;
    }

    public void setQueryPort(String queryPort) {
        this.queryPort = queryPort;
    }



    public String getKeyToSearch() {
        return keyToSearch;
    }

    public void setKeyToSearch(String keyToSearch) {
        this.keyToSearch = keyToSearch;
    }

    public String getAssociatedPort() {
        return associatedPort;
    }

    public void setAssociatedPort(String associatedPort) {
        this.associatedPort = associatedPort;
    }



    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public String getConnectedClients() {
        return connectedClients;
    }

    public void setConnectedClients(String[] connectedClients) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<connectedClients.length;i++){
            String client = connectedClients[i];
            sb.append(client);
            if(i!=connectedClients.length-1){
                sb.append(",");
            }
        }
//        for(String client : connectedClients) {
//            sb.append(client);
//            sb.append(",");
//        }
        this.connectedClients = sb.toString();
    }

    String connectedClients=null;

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }


    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }


    public Message(String type){
        messageType = type;
    }

    public String getString() {
        StringBuilder sb = new StringBuilder();
        if(this.messageType != null) {
            sb.append("messageType:" + this.messageType);
            //sb.append(this.messageType);
            sb.append(";");
        }
        if(this.client != null) {
            sb.append("client:" + this.client);
            sb.append(";");
        }
        if(this.connectedClients != null) {
            sb.append("connectedClinets:" + this.connectedClients);
        }
        if(this.key != null) {
            sb.append("key:" + this.key);
            sb.append(";");
        }
        if(this.value != null) {
            sb.append("value:" + this.value);
            sb.append(";");
        }
        if(this.associatedPort != null) {
            sb.append("associatedPort:" + this.associatedPort);
            sb.append(";");
        }
        if(this.keyToSearch != null){
            sb.append("keyToSearch:" + this.keyToSearch);
            sb.append(";");
        }
        if(this.queryPort != null) {
            sb.append("queryPort:" + this.queryPort);
            sb.append(";");
        }
        if(this.queryAnswer != null) {
            sb.append("queryAnswer:" + this.queryAnswer);
            sb.append(";");
        }
        if(this.GDUMP_Response !=  null) {
            sb.append("gdumpAnswer:" + this.GDUMP_Response);
            sb.append(";");
        }
        if(this.currentPort != null) {
            sb.append("currentPort:" + this.currentPort);
            sb.append(";");
        }
        return sb.toString();
    }
}
