package it.distributedsystems.messages.raft;

public class UniqueMessageIdentifier {
    public int clientId;
    public int commandId;

    public UniqueMessageIdentifier(int clientId, int commandId) {
        this.clientId = clientId;
        this.commandId = commandId;
    }

    public UniqueMessageIdentifier(String formattedVersion) {
        var parts = formattedVersion.split(":");
        this.clientId = Integer.parseInt(parts[0]);
        this.commandId = Integer.parseInt(parts[1]);
    }

    @Override
    public String toString(){
        return String.format("%d:%d", clientId, commandId);
    }
}
