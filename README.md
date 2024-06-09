# traffic-jam

## Projektbeschreibung
Es soll ein Navigationssystem mit Staumeldung simuliert werden. 
Dazu wird ein Graph-Netzwerk in Spark erstellt, welches 4 Wege von Zürich nach Basel kennt. 
Diese Wege haben Zeitdauern hinterlegt, wie lange der Weg ohne Stau dauert.

Der Staumelder meldet nun im 10 Sekunden abstand neue Staus auf den Wegen. Dabei wird immer
wieder ein Weg ausgewählt und eine neue Staudauer anhand einer Sinusfunktion berechnet. Dadurch
nimmt der Stau auf der Strecke kontinuierlich zu und wieder ab.

Bei jeder Staumeldung wird der kürzeste Weg neu berechnet und ausgegeben.
