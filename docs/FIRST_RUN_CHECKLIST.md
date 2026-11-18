# Checklist prima rulare (personal, șterge fișierul înainte să publici repo-ul)

Codul SQL a fost scris pe un laptop fără acces la Snowflake, deci **nu a fost rulat niciodată pe Snowflake**.
Generatorul Python și testele lui au fost rulate. Verifică în ordine și notează ce trebuie reparat.

## 1. Ordinea de rulare
Vezi secțiunea *How to run* din README. Dacă un script pică, repară-l și rulează-l din nou: scripturile sunt făcute să poată fi re-rulate (`IF NOT EXISTS` / `CREATE OR REPLACE`).

## 2. Valori așteptate (generator cu setările implicite, seed 42)
Valorile pot varia ușor dacă versiunea de Python e diferită. Dacă diferă mult, e semn de bug.

| Verificare | Așteptat |
|---|---|
| `RAW_SHIPMENTS` după încărcarea completă | 96.698 rânduri |
| `RAW_SHIPMENT_EVENTS` după încărcarea completă | ≈ 663.022 (660.373 + 2.649 duplicate) |
| `STG_SHIPMENT_EVENTS` | ≈ 660.373 |
| `info: duplicate raw events removed` | ≈ 2.649 |
| `info: late-arriving events` | ≈ 1.933 |
| `FCT_SHIPMENT` DELIVERED / RETURNED / IN_TRANSIT | ≈ 95.264 / 347 / 1.087 |
| On-time (colete livrate) | ≈ 94,9 % global; EXPRESS ≈ 86 %, STANDARD ≈ 97 % |
| Livrate din prima încercare | ≈ 91,3 % |
| Colete create pe 2025-11-28 (Black Friday) | 1.508 |
| On-time pentru coletele create pe 2025-12-01 | ≈ 66 % (cea mai slabă zi) |
| Zile peste capacitate (`FCT_HUB_DAILY`, `CAPACITY_UTILISATION > 1`), per hub | 3 – 17 din 180 (nu zeci de zile) |
| Rezultat Q5 din `07_analytics_queries.sql` | on-time scade de la ≈ 96 % (< 90 % utilizare) la ≈ 85 % (peste capacitate) |
| După încărcarea doar a lunii octombrie (pasul 5) | 15.903 colete, 105.569 evenimente brute / 105.144 în staging |
| Toate regulile `ERROR` din `V_DQ_CHECKS` | PASS |

## 3. Locuri unde probabil apar erori de sintaxă (nu am putut testa)
1. `02_staging.sql` și `03_marts.sql`: procedurile Snowflake Scripting (`LET ... := SQLROWCOUNT`, `:variabila` în `INSERT`).
2. `01_raw_tables_and_load.sql`: `COPY INTO` cu `PATTERN` după un `SELECT` din stage; dacă fișierele sunt comprimate (`.csv.gz`), pattern-ul `.*shipments_2025-10-.*` le prinde.
3. `04_streams_tasks.sql`: dacă `ALTER TASK ... RESUME` dă eroare de privilegii, rulează `GRANT EXECUTE TASK ON ACCOUNT TO ROLE LOGISTICS_ENGINEER;` ca `ACCOUNTADMIN`.
4. `06_ml_forecast_and_anomalies.sql`: sintaxa ML (`TABLE(view)`, `LABEL_COLNAME => ''`, `CONFIG_OBJECT`) a fost verificată în documentația Snowflake, dar nu rulată. Antrenarea cere date regulate zilnic fără goluri. Dacă primești eroare de frecvență, verifică `FCT_HUB_DAILY` (o linie pe hub pe zi).
5. Dacă `V_DQ_CHECKS` arată `FAIL`, nu ignora: fie e un bug în SQL, fie în generator. Pe ambele ar trebui să le repari și să notezi în README ce ai învățat.

## 4. După ce merge, completează partea care face diferența la interviu
- Adaugă în `docs/` 2-3 capturi de ecran: rezultatul `V_DQ_CHECKS`, task history, anomaliile detectate.
- Adaugă în README o secțiune **Results** cu cifrele tale reale: WAPE-ul forecast-ului, zilele detectate ca anomalii, hub-urile cu cele mai multe zile peste capacitate.
- Adaugă o secțiune **What I learned / what I would do differently**.
- Suspendă task-urile (`ALTER TASK ... SUSPEND`) ca să nu consumi credite.
