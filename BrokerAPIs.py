"""BrokerAPI (module)

Modul zum einheitlichen bedienen der APIs unterschiedlicher Broker."""

# Module importieren
# ------------------
from dataclasses import dataclass
from datetime import datetime
import threading
import json
import http.client
import pandas as pd
from dynaconf import Dynaconf, Validator


class CapitalCom:
    """CapitalCom (object)

    Erzeugt eine neue Klasse um eine Verbindung zu Capital.com
    aufzubauen und die API zu bedienen.
    """

    # Private Variablen festlegen
    @dataclass
    class __User:  # pylint: disable=invalid-name
        """Informationen den Benutzer betreffend"""

        sName: str = ""  # Username
        sPassword: str = ""  # API Passwort
        sAPIKey: str = ""  # API Key

    @dataclass
    class __API:  # pylint: disable=invalid-name
        """Informationen die API betreffend"""

        sConnectionURL: str = ""  # Verbindungsstring
        bTest: bool = True  # Testumgebung
        bConnected: bool = False  # Wurde eine Verbindung aufgebaut?
        dHeader: dict = {}  # Headervariablen, die übergeben wurden
        dToken: dict = {}  # Security-Token der Session
        iPingPeriod: float = 300.0  # Dauer zwischen den Ping Wiederholungen

    __pbDebug: bool = False  # Soll Debug-Output ausgegeben werden?
    __pthrThread: threading.Timer  # Thread mit Timer
    keepalive = True  # Soll ein Keepalive gesendet werden?

    # Konstruktor
    def __init__(self):
        """Initialisiert die Klasse und liest die Verbindungs-
        informationen aus der Datei settings.yaml aus.
        """

        # Settings einlesen
        # -----------------
        settings = Dynaconf(
            validators=[
                #        settings.validators.register(
                # Folgende Parameter müssen existieren
                # ------------------------------------
                Validator("USER", "APIPASSWORD", "APIKEY", must_exist=True),
                # Ensure some parameter mets a condition
                # conditions: (eq, ne, lt, gt, lte, gte, identity,
                # is_type_of, is_in, is_not_in)
                # ------------------------------------------------
                Validator("ENVIRONMENT", is_in=["test", "live"]),
            ]
        )

        # Einstellungen abspeichern
        # -------------------------
        self.__User.sName = settings.CONNECTION["USER"]  # type: ignore
        self.__User.sPassword = settings.CONNECTION["APIPASSWORD"]  # type: ignore
        self.__User.sAPIKey = settings.CONNECTION["APIKEY"]  # type: ignore
        self.__API.bTest = settings.CONNECTION["ENVIRONMENT"] == "test"  # type: ignore

        # Wenn wir im Testenvironment sind, dann den Connectionstring
        # entsprechend setzen
        # -----------------------------------------------------------
        if self.__API.bTest:
            self.__API.sConnectionURL = "demo-api-capital.backend-capital.com"
        else:
            self.__API.sConnectionURL = "api-capital.backend-capital.com"

    def __del__(self):
        """Baut die Verbindung zu Capital.com wieder ab."""

        print("Destructor")

    def __clear_vars__(self):
        """Zurücksetzen der beim Verbinden zu Capital.com
        befüllten Variablen."""

        # Header zurücksetzen
        # -------------------
        self.__API.dHeader = {}

        # Token und CST zurücksetzen
        # --------------------------
        self.__API.dToken = {}

        # Verbindung zurücksetzen
        # -----------------------
        self.__API.bConnected = False

        # Keepalives zurücksetzen
        # -----------------------
        self.keepalive = False

    def __keepalive__(self):
        """Sendet kontinuierliche Keepalives"""

        # Wenn keine Verbindung besteht oder keine Keepalives
        # gesendet werden sollen, dann Funktion jetzt beenden
        if not self.keepalive or not self.__API.bConnected:
            return

        # Ping senden
        self.Ping()

        # Vor Ablauf des Timeouts ein Ping absenden
        # Wird alle 60 Sekunden aufgerufen
        # -----------------------------------------
        self.__pthrThread = threading.Timer(  # pylint: disable=invalid-name
            self.__API.iPingPeriod, self.__keepalive__
        )
        self.__pthrThread.start()

    def SessionNew(self):
        """Stellt eine neue Verbindung zu Capital.com her mit den
        Verbindungsangaben, die in settings.yaml abgelegt sind
        und speichert den Security-Token und CST ab."""

        # Start new Session
        payload = {"identifier": self.__User.sName, "password": self.__User.sPassword}
        payload = json.dumps(payload)

        headers = {
            "X-CAP-API-KEY": self.__User.sAPIKey,
            "Content-Type": "application/json",
        }

        # Create Connection
        conn = http.client.HTTPSConnection(self.__API.sConnectionURL)

        # Send Request
        conn.request("POST", "/api/v1/session", payload, headers)
        res = conn.getresponse()

        # Header merken um CST und Security Token auszulesen
        myheader = res.getheaders()

        # Header ist eine Liste von Tuples
        # Umwandeln in Dict
        self.__API.dHeader = dict(myheader)

        # Token und CST in Headervariable merken, die in weiterer Folge
        # bei Aufrufen übermittelt wird.
        self.__API.dToken = {
            "X-SECURITY-TOKEN": self.__API.dHeader["X-SECURITY-TOKEN"],
            "CST": self.__API.dHeader["CST"],
        }

        # Merken, dass eine Verbindung aufgebaut wurde und
        # Keepalives gesendet werden sollen
        self.__API.bConnected = True
        self.keepalive = True

        # Keepalive starten
        # -----------------
        self.__pthrThread = threading.Timer(  # pylint: disable=invalid-name
            self.__API.iPingPeriod, self.__keepalive__
        )
        self.__pthrThread.start()

    def SessionEnd(self):
        """Beendet eine bestehende Verbindung zu Capital.com"""

        # Wenn keine Verbindung besteht, dass alle Variablen leeren
        # und Script beenden
        if not self.__API.bConnected:
            self.__clear_vars__()
            return

        # Create Connection
        conn = http.client.HTTPSConnection(self.__API.sConnectionURL)

        # Payload leeren
        payload = ""

        # Send Request
        conn.request("DELETE", "/api/v1/session", payload, self.__API.dToken)

        # Variablen löschen
        self.__clear_vars__()

    def Ping(self):
        """Baut eine kurze Verbindung zu Capital.com auf um
        ein Ping zu senden."""

        # Wenn keine Verbindung aufgebaut ist, dann Funktion beenden
        if not self.__API.bConnected:
            return

        # Create Connection
        conn = http.client.HTTPSConnection(self.__API.sConnectionURL)

        # Payload leeren
        payload = ""
        ldJsonData = ""

        # Send Request
        conn.request("GET", "/api/v1/ping", payload, self.__API.dToken)

        # Antwort auslesen
        res = conn.getresponse()
        data = res.read()

        # Daten im JSON Format einlesen
        ldJsonData = json.loads(data.decode("utf-8"))

        print(ldJsonData)

    def Kill(self):
        """Beendet die aktuelle Verbindung und löscht die Instanz."""

        # Thread beenden
        del self.__pthrThread

        # Variablen zurücksetzen
        self.keepalive = False
        self.SessionEnd()
        del self

    def GetPrice(
        self, epic: str, resolution: str, start: str, end: str = ""
    ) -> pd.DataFrame:
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

        # Variablendefinition
        # -------------------
        x: int = 0

        # Parameter überprüfen
        lsParameter = "MINUTE, MINUTE_5, MINUTE_15, MINUTE_30, HOUR, HOUR_4, DAY, WEEK"
        x = lsParameter.find(resolution)
        if x < 0:
            raise TypeError(
                "Als Werte für resolution sind nur folgende "
                + f"Werte erlaubt: {lsParameter}"
            )

        # Startwert überprüfen
        datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")

        # Wenn ein Endwert übergeben wurde, dann Endwert überprüfen
        if end != "":
            datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")

        # Create Connection
        conn = http.client.HTTPSConnection(self.__API.sConnectionURL)

        lDf = ""
        lsPayload: str = ""
        lsStartDate = start
        if end != "":
            lsEndDate = end
        else:
            lsEndDate = ""

        # Schleife solange Daten geladen werden können
        while True:
            # Request zusammenstellen
            lsRequest = (
                "/api/v1/prices/"
                + epic
                + "?resolution="
                + resolution
                + "&max=1000&from="
                + lsStartDate
            )

            # Wenn ein Enddatum angegeben wurde, dann dem Request hinzufügen
            if lsEndDate != "":
                lsRequest += "&to=" + lsEndDate

            # Send Request
            conn.request("GET", lsRequest, lsPayload, self.__API.dToken)

            # Antwort einlesen
            res = conn.getresponse()
            data = res.read()

            # Daten im JSON Format einlesen
            ldJsonData = json.loads(data.decode("utf-8"))

            # Spalte 'prices' normalisieren
            lDf1 = pd.json_normalize(ldJsonData, record_path=["prices"])

            # Wenn in df bereits Daten vorhanden sind, dann die Daten hinzufügen
            if len(lDf) > 0:
                oldDf = lDf
                # Ignoreindex führt den Index fort
                # --------------------------------
                lDf = pd.concat([oldDf, lDf1], ignore_index=True)
            else:
                lDf = lDf1

            # print(len(df))
            print(".", end="")

            # Wenn weniger als 1000 Zeilen heruntergeladen wurden, dann den nächsten
            # Block anfragen andernfalls ist die While Schleife zu beenden
            if len(lDf1) < 1000:
                break
            else:
                lsStartDate = lDf1.snapshotTime[999]

        # Index auf Datetime stellen
        lsFormat = "%Y-%m-%dT%H:%M:%S"
        lDf["index"] = pd.to_datetime(lDf["snapshotTime"], format=lsFormat)
        lDf = lDf.set_index(pd.DatetimeIndex(lDf["index"]))

        # Dataframe zurückgeben
        return lDf
