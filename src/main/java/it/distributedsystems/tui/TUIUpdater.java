package it.distributedsystems.tui;

public class TUIUpdater {
    /**
     * This method is called everytime something new must be shown in the view
     * Starts a single thread (cannot be started multiple consecutive times because if a thread is already running then will probably include the second change without starting another thread)
     */
    public static void reprintViewAsync() {
        //Se c'e' un thread gia' in esecuzione, RETURN senza startare
        //Starta un thread che fa printViewInternal
    }

    /**
     * Actually reprints the view.
     */
    private static void printViewInternal(){

    }
}
