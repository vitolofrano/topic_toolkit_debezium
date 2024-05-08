import psycopg2
from confluent_kafka.admin import AdminClient, NewTopic
import sys

def ascii_art():
    print("\033[95m" + """
 _________  ________  ________  ___  ________                         
|\\___   ___\\\\   __  \\|\\   __  \\|\\  \\|\\   ____\\                        
\\|___ \\  \\_\\ \\  \\|\\  \\ \\  \\|\\  \\ \\  \\ \\  \\___|                        
     \\ \\  \\ \\ \\  \\\\\\  \\ \\   ____\\ \\  \\ \\  \\                           
      \\ \\  \\ \\ \\  \\\\\\  \\ \\  \\___|\\ \\  \\ \\  \\____                      
       \\ \\__\\ \\ \\_______\\ \\__\\    \\ \\__\\ \\_______\\                    
        \\|__|  \\|_______|\\|__|     \\|__|\\|_______|                    

 _________  ________  ________  ___       ___  __    ___  _________   
|\\___   ___\\\\   __  \\|\\   __  \\|\\  \\     |\\  \\|\\  \\ |\\  \\|\\___   ___\\ 
\\|___ \\  \\_\\ \\  \\|\\  \\ \\  \\|\\  \\ \\  \\    \\ \\  \\/  /|\\ \\  \\|___ \\  \\_| 
     \\ \\  \\ \\ \\  \\\\\\  \\ \\  \\\\\\  \\ \\  \\    \\ \\   ___  \\ \\  \\   \\ \\  \\  
      \\ \\  \\ \\ \\  \\\\\\  \\ \\  \\\\\\  \\ \\  \\____\\ \\  \\\\ \\  \\ \\  \\   \\ \\  \\ 
       \\ \\__\\ \\ \\_______\\ \\_______\\ \\_______\\ \\__\\\\ \\__\\ \\__\\   \\ \\__\\
        \\|__|  \\|_______|\\|_______|\\|_______|\\|__| \\|__|\\|__|    \\|__|
""" + "\033[0m")




def get_tables_without_aud(host, port, dbname, user, password):
    try:
        # Connessione al database
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        cursor = conn.cursor()

        # Ottieni tutti i nomi delle tabelle nel database
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
        tables = cursor.fetchall()

        # Filtra le tabelle che non terminano con "AUD"
        filtered_tables = [table[0] for table in tables if not table[0].endswith('aud')]

        # Chiudi la connessione al database
        conn.close()

        return filtered_tables

    except psycopg2.Error as e:
        print("\033[91m" +"Errore durante la connessione al database:"+ "\033[0m", e)
        sys.exit(1)
        return []

def create_kafka_topics(broker, topics, num_partitions, greenup, compression, cleanup):
    flag = 0
    try:
        admin_client = AdminClient({'bootstrap.servers': broker})

        for topic in topics:
            topic_name = f"{slug}.{dbname}.{topic}.v1"
            config = {
                "cleanup.policy": cleanup,
                "compression.type": compression,
                "min.insync.replicas": greenup
            }
            new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=1, config=config)
            creation = admin_client.create_topics([new_topic])
            for topic, result in creation.items():
                try:
                    result.result()  # Attendere la conferma della creazione
                    print(f"Topic '{topic}' creato con successo.")
                except Exception as e:
                    flag = 1
                    print("\033[91m" +f"Errore durante la creazione del topic '{topic}': {e}" + "\033[0m")

        if(flag == 0):
            print("\033[92m" + " \n \n Topic Kafka creati con successo." + "\033[0m")

    except Exception as e:
        print("\033[91m" +"Errore durante la creazione dei topic Kafka:"+ "\033[0m", e)

def delete_sm2_topics(kafka_servers):
    flag = 2
    try:
        # Creazione del client AdminClient
        admin_client = AdminClient({'bootstrap.servers': kafka_servers})

        # Ottieni tutti i topic
        topics = admin_client.list_topics().topics

        # Trova e elimina solo i topic che iniziano con "slug"
        for topic in topics:
            if topic.startswith(f"{slug}"):
                deletion = admin_client.delete_topics([topic])
                for topic, result in deletion.items():
                    try:
                        result.result()  # Attendere la conferma della rimozione
                        print(f"Topic '{topic}' eliminato con successo.")
                        flag = 0
                    except Exception as e:
                        flag  =  1
                        print("\033[91m" +f"Errore durante l'eliminazione del topic '{topic}': {e}"+ "\033[0m")

        if (flag == 0):
            print("\033[92m" +f"Tutti i topic Kafka che iniziano con '{slug}' sono stati eliminati con successo."+ "\033[0m")
        else:
            print("\033[91m" +"Errore durante l'eliminazione dei topic Kafka: non ci sono topic adeguati"+ "\033[0m")
    except Exception as e:
        print("\033[91m" +"Errore durante l'eliminazione dei topic Kafka:"+ "\033[0m", e)

def delete_topics_for_tables(kafka_servers, table_names):
    try:
        # Creazione del client AdminClient
        admin_client = AdminClient({'bootstrap.servers': kafka_servers})

        # Ottieni tutti i topic
        topics = admin_client.list_topics().topics

        # Trova e elimina solo i topic corrispondenti alle tabelle specificate
        for table_name in table_names:
            topic_name = f"{slug}.{dbname}.{table_name}.v1"
            if topic_name in topics:
                print(topic_name)
                deletion = admin_client.delete_topics([topic_name])
                for topic, result in deletion.items():
                    try:
                        result.result()  # Attendere la conferma della rimozione
                        print(f"Topic '{topic}' eliminato con successo.")
                    except Exception as e:
                        print("\033[91m" +f"Errore durante l'eliminazione del topic '{topic}': {e}"+ "\033[0m")

        print("\033[92m" +"Tutti i topic corrispondenti alle tabelle specificate sono stati eliminati con successo."+ "\033[0m")

    except Exception as e:
        print("\033[91m" +"Errore durante l'eliminazione dei topic:"+ "\033[0m", e)

def slang_dbname():
    global dbname
    slang = input("dichiara  uno slang per il tuo dbname ($slug.$dbname.$tablename) o lascia vuoto: ")
    if (len(slang)!=0):
        dbname=slang


def connection_table_name():
    global dbname
    global slug
    if len(sys.argv) != 6 and len(sys.argv) != 1 :
        print("\033[91m" +"Usage: python script.py <host> <port> <dbname> <user> <password>"+ "\033[0m")
        sys.exit(1)
    elif len(sys.argv) == 6:
        host = sys.argv[1]
        port = sys.argv[2]
        dbname = sys.argv[3]
        user = sys.argv[4]
        password = sys.argv[5]
    else :
        host = input("Inserisci l'host del database PostgreSQL: ")
        port = input("Inserisci la porta del database PostgreSQL: ")
        dbname = input("Inserisci il nome del database PostgreSQL: ")
        user = input("Inserisci il nome utente del database PostgreSQL: ")
        password = input("Inserisci la password del database PostgreSQL: ")

    # Ottieni i nomi delle tabelle senza AUD
    table_names = get_tables_without_aud(host, port, dbname, user, password)
    return  table_names



if __name__ == "__main__":
    global slug
    ascii_art()
    print("\n\n\n")
    # Richiesta delle configurazioni dei topic Kafka
    broker = input("Inserisci l'indirizzo del broker Kafka: ")
    slug = input("Scegli lo slug che utilizzi come pattern ($slug.$dbname.tablename): ")
    scelta = input("\n 1. elimina tutti i topic \n 2.crea i topic  \n 3.elimina i topic per un db \n 0.esci \n scegli : ")
    while(scelta != "0") :
        if ( scelta == "1") :
            delete_sm2_topics(broker)
        elif(scelta == "2") :
            table_names = connection_table_name()
            slang_dbname()
            partition = int(input("Inserisci il numero di partizioni per i topic: "))
            greenup = int(input("Inserisci il valore per min.insync.replicas: "))
            compression = input("Inserisci il tipo di compressione (es. gzip, snappy, none): ")
            cleanup = input("Inserisci la politica di cleanup (es. delete, compact): ")

            # Crea i topic Kafka per ogni tabella con le configurazioni specificate
            create_kafka_topics(broker, table_names, partition, greenup, compression, cleanup)
        elif (scelta == "3") :
            table_names = connection_table_name()
            slang_dbname()
            delete_topics_for_tables(broker,table_names)
        else:
            print("Scelta non valida , Riprova")
        scelta = input("\n 1. elimina tutti i topic \n 2.crea i topic  \n 3.elimina i topic per un db \n 0.esci \n scegli : ")
