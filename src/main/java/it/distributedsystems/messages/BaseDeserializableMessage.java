package it.distributedsystems.messages;

import com.google.gson.Gson;

/**
 * Base message, every message extends it, needed for deserialization
 */
public class BaseDeserializableMessage {
    private final MessageDeserializerType deserializerType;

    public BaseDeserializableMessage(MessageDeserializerType deserializerType) {
        this.deserializerType = deserializerType;
    }

    public MessageDeserializerType getDeserializerType() {
        return deserializerType;
    }

    /**
     * Method to convert current class to json
     * @return the String of json representation
     */
    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
