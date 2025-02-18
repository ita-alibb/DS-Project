package it.distributedsystems.messages;

import com.google.gson.Gson;
import it.distributedsystems.messages.queue.*;
import it.distributedsystems.messages.raft.AppendEntries;

public class GsonDeserializer {
    /**
     * Global deserializer, then needs to check for instance to get the type
     */
    public static BaseDeserializableMessage deserialize(String json) {
        Gson gson = new Gson();
        var type = gson.fromJson(json, BaseDeserializableMessage.class).getDeserializerType();

        return switch (type) {
            case CONNECTION_MESSAGE -> gson.fromJson(json, ConnectionMessage.class);
            case QUEUE_COMMAND -> gson.fromJson(json, QueueCommand.class);
            case QUEUE_RESPONSE -> gson.fromJson(json, QueueResponse.class);
            case CONNECTION_RESPONSE -> gson.fromJson(json, ConnectionResponse.class);
            case APPEND_ENTRIES -> gson.fromJson(json, AppendEntries.class);
        };
    }
}
