import socket #socket pour la communication réseau.
import time   #time pour insérer des pauses entre les envois.
import re     #

# Constantes
HOST = '0.0.0.0'  
PORT = 9998 #9999 pour spark ?     
LOG_FILE = '/data/web_server.log' # Chemin vers les logs


def get_log_time(log):
    """Récupère le timestamp d'un log en secondes."""
    
    timestamp_regex = r'\[\d{2}/[A-Za-z]{3}/\d{4}:(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2}) \+\d{4}\]'
    match = re.search(timestamp_regex, log)

    if match:
        h = int(match.group('h'))
        m = int(match.group('m'))
        s = int(match.group('s'))
    else:
        raise ValueError(f"Le timestamp du log suivant est mal formaté :\n'{log}'")
       
    return ((h * 60) + m) * 60 + s


def generate_data():
    """Génère des logs pendant un temps donné."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #Création d’un socket TCP.
    server_socket.bind((HOST, PORT)) # Association à l’adresse et au port définis.
    server_socket.listen(1) # Mise en écoute des connexions entrantes.

    print(f"[Generator] Listening for connections on {HOST}:{PORT}...", flush=True) 

    conn, addr = server_socket.accept() # Attente d’une connexion
    print(f"[Generator] Connection from {addr}", flush=True) # Affichage de l’adresse du client.
        
    try:
        while True:
            sec = None  # on a pas encore récupéré le timestamp
            with open(LOG_FILE, 'r') as f:
                for line in f:
                    line_sec = get_log_time(line)
                    if (sec is not None) and (line_sec != sec) : # si on est toujours au même timestamp
                        time.sleep(line_sec - sec) # on attend le temps du timedelta
                    conn.send(line.encode('utf-8'))  # on affiche la ligne
                    sec = line_sec
                    
            print("[Generator] EOF - start over reading", flush=True)
    #Capture des erreurs et fermeture propre de la connexion en cas d’interruption.
    except IOError:
        print("[Generator] Connection lost.")
    except Exception as e:
        print(f"[Generator] Error: {e}")
    finally:
        conn.close()
        server_socket.close()
        print(f"[Generator] Stopped.")


if __name__ == "__main__":
    print("Starting data-generator.", flush=True)
    # Boucle de service : si la fonction s'arrête, on la relance
    while True:
        try:
            generate_data()
        except KeyboardInterrupt:
            print("Stopping data generator.")
            break
        except Exception as e:
            print(f"Critical Error: {e}")
        
        # Petite pause avant de redémarrer le serveur
        print("Restarting the server in 1 second...", flush=True)
        time.sleep(1)