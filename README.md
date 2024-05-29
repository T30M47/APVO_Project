<h1>Upute za pokretanje</h1>
<hr>
Prvo preuzmite zip i otvorite mapu ***** u željenom uređivaču koda.<br>
Također, repozitorij možete i klonirati putem terminala željenog uređivača koda s naredbom:
<br>

```
git clone https://github.com/T30M47/APVO_Project.git
```

Zatim se pozicionirajte u mapu gdje se nalazi Dockerfile (*****):
```
cd ./******
```

Zatim u terminalu pokrenite naredbu i pričekajte da se podignu kontejneri (traje nešto duže zbog popunjavanja baza podataka):
```
docker-compose up --build
```
Nakon što se Vam se pokrenuli kontejneri, u novom terminalu (pazite da ste i dalje u folderu gdje je Docker file), pokrenite ETL procese s naredbom:
```
docker exec spark spark-submit --master local[*] --driver-class-path /app/postgresql-42.7.1.jar /app/warehouse.py
```
Zadnji korak je pokrenuti web aplikaciju s naredbom:
```
Nakon toga aplikacija postaje dostupna na:
```
localhost:8050
```
docker exec dash_web_app python Dash/app.py
```
<h1>Upute za korištenje</h1>
<hr>

- Na stranici Analitika moguće je izabrati godinu iz padajućeg izbornika te pregledati analitičke podatke o prodaji i transakcijama.
- Na stranici Predviđanja moguće je, iz padajućeg izbornika na vrhu, birati vremenski raspon predviđanja (30, 183 ili 365 dana). Također, iz drugog padajućeg izbornika moguće je odabrati jedan od top 5 najprodavanijih proizvoda te će se prikazati predviđanje njegove preodaje za odabrani vremenski raspon iz prvog padajućeg izbornika.

