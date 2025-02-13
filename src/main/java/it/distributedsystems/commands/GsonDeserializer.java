package it.distributedsystems.commands;

import com.google.gson.Gson;

public class GsonDeserializer {
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

        if (dataResponse.getData() == Integer.MIN_VALUE){
            return gson.fromJson(json, BaseResponse.class);
        }

        return dataResponse;
    }
}
