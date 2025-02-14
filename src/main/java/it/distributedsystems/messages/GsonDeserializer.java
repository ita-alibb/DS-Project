package it.distributedsystems.messages;

import com.google.gson.Gson;
import it.distributedsystems.messages.broker.BrokerCommand;
import it.distributedsystems.messages.client.ClientCommand;
import it.distributedsystems.messages.client.CommandType;
import it.distributedsystems.messages.client.DataResponse;
import it.distributedsystems.messages.newmessages.*;

import java.io.InvalidObjectException;

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
            case REDIRECT_RESPONSE -> gson.fromJson(json, RedirectResponse.class);
        };
    }
/*
    public static BaseCommand deserializeCommand(String json) {
        Gson gson = new Gson();
        var baseCommand = gson.fromJson(json, BaseCommand.class);
        if (baseCommand.isBroker()){
            return gson.fromJson(json, BrokerCommand.class);
        } else {
            return gson.fromJson(json, ClientCommand.class);
        }
    }

    public static BaseResponse deserializeResponse(String json) {
        Gson gson = new Gson();
        var dataResponse = gson.fromJson(json, DataResponse.class);

        if (dataResponse.getData() == null){
            return gson.fromJson(json, BaseResponse.class);
        }

        return dataResponse;
    }*/
}
