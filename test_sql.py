from backend.sql_db import get_facilities_by_city, format_facilities
from backend.sql_db import get_free_places, reserve_place

def main():
    city = "Linz"
    rows = get_facilities_by_city(city)
    print(format_facilities(rows, city))

if __name__ == "__main__":
    main()

fid = "…deine-facility-id…"
print("freie Plätze vorher:", get_free_places(fid))
ok = reserve_place(fid, "Max Mustermann", "max@example.com")
print("erfolgreich?", ok)
print("freie Plätze nachher:", get_free_places(fid))