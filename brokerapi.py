# Module importieren
#-------------------
import pandas as pd

class capitalcom:
    """capitalcom (object)
    
    Erzeugt eine neue Klasse um eine Verbindung zu Capital.com
    aufzubauen und die API zu bedienen.
    """

    # Private Variablen festlegen
    __psUser = ""             # Username
    __psAPIPassword = ""      # API Passwort
    __psAPIKey = ""           # API Key
    __psConnectionURL = ""    # Verbindungsstring
    __pdictHeader = dict()    # Headervariablen, die übergeben wurden
    __pdictToken = dict()     # Security-Token der Session
    __piPingPeriod = 300.0    # Dauer zwischen den Ping Wiederholungen
    __pbConnected = False     # Wurde eine Verbindung aufgebaut?
    __pbTest = False          # Testumgebung
    __pbDebug = False         # Soll Debug-Output ausgegeben werden?
    __pthrThread = ""         # Thread mit Timer
    keepalive = True          # Soll ein Keepalive gesendet werden?
    
    # Konstruktor
    def __init__(self):
        """Initialisiert die Klasse und liest die Verbindungs-
        informationen aus der Datei settings.yaml aus.
        """
        
        # Konfigurationseinstellungen importieren
        from dynaconf import settings, Validator

        # Validator der Settings konfigurieren
        settings.validators.register(
            
            # Folgende Parameter müssen existieren
            Validator('USER', 'APIPASSWORD', 'APIKEY', must_exist=True),

            # Ensure some parameter mets a condition
            # conditions: (eq, ne, lt, gt, lte, gte, identity, is_type_of, is_in, is_not_in)
            Validator('ENVIRONMENT', is_in=['test','live']),

        )        
        
        # Einstellungen abspeichern
        self.__psUser = settings.CONNECTION["USER"]
        self.__psAPIPassword = settings.CONNECTION["APIPASSWORD"]
        self.__psAPIKey = settings.CONNECTION["APIKEY"]
        self.__pbTest = ( settings.APP["ENVIRONMENT"] == "test" )
        
        # Wenn wir im Testenvironment sind, dann den Connectionstring entsprechend setzen
        if self.__pbTest:
            self.__psConnectionURL = "demo-api-capital.backend-capital.com"
        else:
            self.__psConnectionURL = "api-capital.backend-capital.com"           
            
    def __del__(self):
        """Baut die Verbindung zu Capital.com wieder ab."""
        
        print("Destructor")
                
    def __clear_vars__(self):
        """Zurücksetzen der beim Verbinden zu Capital.com
        befüllten Variablen."""

        # Header zurücksetzen
        self.__dictHeader = ""

        # Token und CST zurücksetzen
        self.__pdictToken = ""

        # Verbindung zurücksetzen
        self.__pbConnected = False
        
        # Keepalives zurücksetzen
        self.keepalive = False
        
    def __keepalive__(self):
        """Sendet kontinuierliche Keepalives"""
        
        # Erforderliche Module laden
        import threading
        
        # Wenn keine Verbindung besteht oder keine Keepalives
        # gesendet werden sollen, dann Funktion jetzt beenden
        if not self.keepalive or not self.__pbConnected:
            return
        
        # Ping senden
        self.ping()
        
        # Vor Ablauf des Timeouts ein Ping absenden
        self.__pthrThread = threading.Timer(self.__piPingPeriod, self.__keepalive__).start() # called every minute        
        
    def session_new(self):
        """Stellt eine neue Verbindung zu Capital.com her mit den
        Verbindungsangaben, die in settings.yaml abgelegt sind
        und speichert den Security-Token und CST ab."""
        
        # Erforderliche Module laden
        import json
        import http.client
        import threading
    
        # Start new Session
        payload = {
            'identifier': self.__psUser, 
            'password': self.__psAPIPassword
        }
        payload = json.dumps(payload)

        headers = {
          'X-CAP-API-KEY': self.__psAPIKey,
          'Content-Type': 'application/json'
        }

        # Create Connection
        conn = http.client.HTTPSConnection(self.__psConnectionURL)

        # Send Request
        conn.request("POST", "/api/v1/session", payload, headers)
        res = conn.getresponse()

        # Header merken um CST und Security Token auszulesen
        myheader = res.getheaders()

        # Header ist eine Liste von Tuples
        # Umwandeln in Dict
        self.__dictHeader = dict( myheader )

        # Token und CST in Headervariable merken, die in weiterer Folge
        # bei Aufrufen übermittelt wird.
        self.__pdictToken = {
          'X-SECURITY-TOKEN': self.__dictHeader["X-SECURITY-TOKEN"],
          'CST': self.__dictHeader["CST"]
        }

        # Merken, dass eine Verbindung aufgebaut wurde und 
        # Keepalives gesendet werden sollen
        self.__pbConnected = True
        self.keepalive = True
        
        # Vor Ablauf des Timeouts ein Ping absenden
        self.__pthrThread = threading.Timer(self.__piPingPeriod, self.__keepalive__).start() # called every minute
    
    def session_end(self):
        """Beendet eine bestehende Verbindung zu Capital.com"""
        
        # Erforderliche Module laden
        import json
        import http.client
    
        # Wenn keine Verbindung besteht, dass alle Variablen leeren
        # und Script beenden
        if not self.__pbConnected:
            self.__clear_vars__()
            return
        
        # Create Connection
        conn = http.client.HTTPSConnection(self.__psConnectionURL)

        # Payload leeren
        payload = ""
        
        # Send Request
        conn.request("DELETE", "/api/v1/session", payload, self.__pdictToken)

        # Antwort auslesen
        res = conn.getresponse()
        data = res.read()

        # Daten im JSON Format einlesen
        ldJsonData = json.loads(data.decode("utf-8"))
        
        # Variablen löschen
        self.__clear_vars__()

    def ping(self):
        """Baut eine kurze Verbindung zu Capital.com auf um 
        ein Ping zu senden."""
        
        # Wenn keine Verbindung aufgebaut ist, dann Funktion beenden
        if not self.__pbConnected:
            return
        
        # Erforderliche Module laden
        import json
        import http.client
    
        # Create Connection
        conn = http.client.HTTPSConnection(self.__psConnectionURL)

        # Payload leeren
        payload = ""
        ldJsonData = ""
        
        # Send Request
        conn.request("GET", "/api/v1/ping", payload, self.__pdictToken)

        # Antwort auslesen
        res = conn.getresponse()
        data = res.read()

        # Daten im JSON Format einlesen
        ldJsonData = json.loads(data.decode("utf-8"))
        
        print(ldJsonData)
        
    def kill(self):
        """Beendet die aktuelle Verbindung und löscht die Instanz."""
        
        # Thread beenden
        del self.__pthrThread
        
        # Variablen zurücksetzen
        self.keepalive = False
        self.session_end()
        del self
    
    def get_price(self, epic:str, resolution:str, start:str, end:str="") -> pd.DataFrame:
        """Liest historische Preisinformation von der API ein
        
        Parameter
        ---------
        epic : str - mandatory
            Das Finanzinstrument von dem die Daten eingelesen werden sollen
        resolution : str - mandatory 
            Die Zeitabstände, in denen die Informationen eingelesen werden sollen.
            Mögliche Werte sind: MINUTE, MINUTE_5, MINUTE_15, MINUTE_30, HOUR, 
            HOUR_4, DAY, WEEK
        start : datetime - mandatory
            Startdatum und -uhrzeit ab dem die Daten eingelesen werden sollen.
            Format: YYYY-MM-DDTHH:MM:SS
        end : datetime - optional
            Enddatum und -uhrzeit bis zu dem die Daten eingelesen werden sollen.
            Wenn kein Enddatum angegeben wurde, dann wird bis zum aktuellen 
            Zeitpunkt eingelesen.
            Format: YYYY-MM-DDTHH:MM:SS"""
        
        # Erforderliche Module laden
        from datetime import datetime
        import json
        import http.client
        import pandas as pd

        # Parameter überprüfen
        lsParameter = "MINUTE, MINUTE_5, MINUTE_15, MINUTE_30, HOUR, HOUR_4, DAY, WEEK"
        x = lsParameter.find(resolution)
        if x < 0:
            raise TypeError(
                'Als Werte für resolution sind nur folgende Werte erlaubt: {}'.format(lsParameter))
        
        # Startwert überprüfen
        datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
        
        # Wenn ein Endwert übergeben wurde, dann Endwert überprüfen
        if end != "":
            datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")

        # Create Connection
        conn = http.client.HTTPSConnection(self.__psConnectionURL)

        df=""
        payload = ""
        lsStartDate = start
        if end != "":
            lsEndDate = end
        else:
            lsEndDate = ""
            
        # Schleife solange Daten geladen werden können
        while True:

            # Request zusammenstellen
            lsRequest = "/api/v1/prices/" + epic + "?resolution=" + resolution + "&max=1000&from=" + lsStartDate
            
            # Wenn ein Enddatum angegeben wurde, dann dem Request hinzufügen
            if lsEndDate != "":
                lsRequest += "&to=" + lsEndDate
            
            # Send Request
            conn.request("GET", lsRequest, payload, self.__pdictToken)

            # Antwort einlesen
            res = conn.getresponse()
            data = res.read()

            # Daten im JSON Format einlesen
            ldJsonData = json.loads(data.decode("utf-8"))
            
            # Spalte 'prices' normalisieren
            df1 = pd.json_normalize(ldJsonData, record_path =['prices'])

            # Wenn in df bereits Daten vorhanden sind, dann die Daten hinzufügen
            if len(df) > 0:
                old_df = df
                df = pd.concat([old_df,df1],ignore_index=True)  # Ignoreindex führt den Index fort
            else:
                df = df1

            #print(len(df))
            print(".", end="")

            # Wenn weniger als 1000 Zeilen heruntergeladen wurden, dann den nächsten Block anfragen
            # andernfalls ist die While Schleife zu beenden
            if ( len(df1) < 1000 ):
                break
            else:
                lsStartDate = df1.snapshotTime[999]
                        
        # Index auf Datetime stellen
        lsFormat = '%Y-%m-%dT%H:%M:%S'
        df['index'] = pd.to_datetime(df['snapshotTime'], format=lsFormat)
        df = df.set_index(pd.DatetimeIndex(df['index']))

        # Dataframe zurückgeben
        return df
