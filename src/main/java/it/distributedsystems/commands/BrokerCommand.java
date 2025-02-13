package it.distributedsystems.commands;

public class BrokerCommand extends BaseCommand{
    /**
     * Contructor of the command
     *
     * @param isBroker if is from a broker or not
     * @param senderID the id of the sender
     */
    public BrokerCommand(boolean isBroker, String senderID) {
        super(isBroker, senderID);
    }
}
