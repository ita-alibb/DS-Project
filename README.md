Questo branch ora lo mergio e poi cancellero' questo readme.
Ci sono molti TODO e molti commenti.
Spiego in breve quello che c'e' nel codice, tutto potrebbe non funzionare, NON e' testato e magari puo' essere fatto meglio.
#### In Connection: sono tutte basate su TCP quindi hanno un thread sempre in ascolto ed uno per scrivere.
ClientConnection e' la connessione dei client, semplicemente si connette ad un broker, e se e' il leader rimane connesso e gli manda i comandi serializzati (ancora manca tutta la parte di prompt dei comandi quindi al momento non si possono inviare)
BrokerConnection: ha 2 server, uno dove si connettono i client ed uno dove si connettono gli altri broker. Quando un broker e' Leader accetta i client e si connette ai Server di tutti gli altri broker (questa parte ancora non fatta). 
Quando il broker e' ollower rimane in ascolto nel server client, ma se riceve una connessione la redirecta al leader; rimane in ascolto nel server dei broker invece per ricevere le AppendEntries del leader e gli eventuali RequestVote di altri broker per cambiare leader.
Manca tutta la gestione dei thread ed il processamento dei messaggi e la gestione delle epoch ecc.
#### In messages:
ci sono tutte le classi dei messaggi. Quelli tra Leader e Client sono tutti fatti, Mancano quelli per la gestione di RAFT. Tutti ereditano da BaseDeserializableMessage e vengono serializzati facento .toJson() e deserializzati con GsonDeserializer.deserialize(msg)
#### In raft:
c'e' il Model interno del broker, ovvero la struttura di queue, sulla quale si possono gia' eseguire i comandi necessari (creare una nuova coda,aggiungere elemento e leggere) ogni client puo' leggere/scrivere ogni coda, le code non hanno "proprietari"
ReplicationLog e' la classe statica che gestisce la scrittura dei log, non l'ho testata ma dovrebbe funzionare, dentro nei commenti vedi la struttura del log. La serializzazione dei messaggi e' fatta.
#### In tui:
l'input reader copiato dal mio vecchio progetto, si deve usare solo nel client (e' l'unico che riceve comandi attivi).
Poi TuiUpdater sarebbe la classe che aggiorna quello che viene mostrato nella TUI, ma non c'e' ancora niente dentro.
#### in fine
ci sono due punt di ingresso uno per il client uno per il broker. Poi c'e' un file di configurazione statico perche' possiamo dare per assunto che i broker si conoscano tra di loro, quindi quel file verra' letto da tutti i broker per "conoscersi" a vicenda.



