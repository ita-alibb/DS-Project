package it.distributedsystems;

import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class GsonTest {
    @Test
    @DisplayName("Test json ConnectionMessage")
    public void ConnectionMessageTest(){
        // Path and filename of the json settings file.
        final String path = "src/test/java/it/distributedsystems/jsonMessages";
        final String jsonFileName = "ConnectionMessage.json";
        final Path jsonFilePath = Path.of(path, jsonFileName);
        // Check if the file exists.
        assertTrue(Files.exists(jsonFilePath));

        // Read the text from file.
        String jsonText = null;
        try {
            jsonText = Files.readString(jsonFilePath);
        } catch (IOException e) {
            assert(false);
        }

        //Test deserialization
        ConnectionMessage message = (ConnectionMessage) GsonDeserializer.deserialize(jsonText);
        assertNotNull(message);

        //Test serialization
        String deserialized = message.toJson();
        assertEquals(jsonText, deserialized);
    }

    @Test
    @DisplayName("Test json ConnectionResponse")
    public void ConnectionResponseTest(){
        // Path and filename of the json settings file.
        final String path = "src/test/java/it/distributedsystems/jsonMessages";
        final String jsonFileName = "ConnectionResponse.json";
        final Path jsonFilePath = Path.of(path, jsonFileName);
        // Check if the file exists.
        assertTrue(Files.exists(jsonFilePath));

        // Read the text from file.
        String jsonText = null;
        try {
            jsonText = Files.readString(jsonFilePath);
        } catch (IOException e) {
            assert(false);
        }

        //Test deserialization
        ConnectionResponse message = (ConnectionResponse) GsonDeserializer.deserialize(jsonText);
        assertNotNull(message);

        //Test serialization
        String deserialized = message.toJson();
        assertEquals(jsonText, deserialized);
    }

    @Test
    @DisplayName("Test json QueueCommand")
    public void QueueCommandTest(){
        // Path and filename of the json settings file.
        final String path = "src/test/java/it/distributedsystems/jsonMessages";
        final String jsonFileName = "QueueCommand.json";
        final Path jsonFilePath = Path.of(path, jsonFileName);
        // Check if the file exists.
        assertTrue(Files.exists(jsonFilePath));

        // Read the text from file.
        String jsonText = null;
        try {
            jsonText = Files.readString(jsonFilePath);
        } catch (IOException e) {
            assert(false);
        }

        //Test deserialization
        QueueCommand message = (QueueCommand) GsonDeserializer.deserialize(jsonText);
        assertNotNull(message);

        //Test serialization
        String deserialized = message.toJson();
        assertEquals(jsonText, deserialized);
    }

    @Test
    @DisplayName("Test json QueueResponse")
    public void QueueResponseTest(){
        // Path and filename of the json settings file.
        final String path = "src/test/java/it/distributedsystems/jsonMessages";
        final String jsonFileName = "QueueResponse.json";
        final Path jsonFilePath = Path.of(path, jsonFileName);
        // Check if the file exists.
        assertTrue(Files.exists(jsonFilePath));

        // Read the text from file.
        String jsonText = null;
        try {
            jsonText = Files.readString(jsonFilePath);
        } catch (IOException e) {
            assert(false);
        }

        //Test deserialization
        QueueResponse message = (QueueResponse) GsonDeserializer.deserialize(jsonText);
        assertNotNull(message);

        //Test serialization
        String deserialized = message.toJson();
        assertEquals(jsonText, deserialized);
    }
}
