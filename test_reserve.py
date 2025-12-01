from backend.sql_db import get_free_places, reserve_place

def main():
    kennzahl = 401102  # Beispiel-Einrichtung: Krabbelstube Allendeplatz

    print("Freie Plätze vorher:", get_free_places(kennzahl))
    ok = reserve_place(kennzahl, "Max Muster", "max@example.com", "Emma Mustermann")
    print("Vormerkung erfolgreich?", ok)
    print("Freie Plätze nachher:", get_free_places(kennzahl))

if __name__ == "__main__":
    main()