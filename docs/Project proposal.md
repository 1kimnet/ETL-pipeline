# **ETL \- OP \- Data pipeline**

## **Projektsammanfattning**

## **Mål**

Utveckla en robust, konfigurerbar och automatiserad ETL-applikation i Python för ArcGIS Server 11.3 och ArcGIS Pro 3.3. Applikationen ska veckovis hämta geografisk data från webbaserade källor från svenska nationella och regionala myndigheter och ladda till en central SQL Server SDE-geodatabas. Endast Python-bibliotek som ingår i standard ArcGIS-installationen får användas.

## **Sammanfattning**

Applikationen körs schemalagt, t.ex. veckovis via Windows Task Scheduler, och följer ett standardiserat ETL-flöde:

* **Extraktion**: Hämtning av data via URL (t.ex. REST, WFS, OGC API, Atom).
* **Staging**: Mellanlagring i temporär File Geodatabase.
* **Transformation**: Bearbetning med ArcPy (t.ex. projicering, klippning).
* **Inladdning**: Överföring till mål-SDE.

Styrningen sker helt via tre YAML-konfigurationsfiler, utformade för att vara begripliga även för icke-tekniska användare.

## **Nyckelfunktioner**

* Konfigurationsdriven arkitektur med stöd för olika datakällor, format (JSON, SHP, GDB, GPKG), geografisk filtrering (BBox), koordinatsystem och återförsök vid fel.
* Modulär uppbyggnad med separata komponenter för nedladdning, staging, transformation och inladdning.
* Omfattande loggning: både läsvänlig sammanfattning, utan felmeddelanden, och en mer detaljerad felsökningslogg.
* Felhantering som möjliggör fortsättning även vid delvisa fel (t.ex. nätverksproblem
* Utilities som hanterar globala funktioner, exempelvis en unzipper som hanterar all eventull uppzippanden, sanitizer som exempelvis sanerar namn vid behov, exempelvis för att svenska tecken. Vali

## **Begränsningar**

Inga externa Python-paket får installeras. Endast bibliotek som finns i ArcGIS-miljön får användas, såsom `requests`, `PyYAML`, `json`, `zipfile`, `logging`, `xml.etree.ElementTree` och `arcpy`.

## **Målgrupp**

Kommunala samhällsplanerare och annan icke-teknisk personal. Därför är konfigurationsfilerna designade för att vara lättförståeliga och redigerbara.

## **Nyckelkomponenter**

* **YAML-konfiguration**: `config.yaml` (loggning, återförsök), `sources.yaml` (per källa), `environment.yaml` (miljöspecifika sökvägar och koordinatsystem).
* **Modulär design**: Klart avgränsade moduler (`downloaders`, `utils`, `staging_loader`, `processor`, `sde_loader`) styrda av ett huvudskript.
* **ETL-process**: Sekventiellt flöde från extraktion till inladdning, med möjlighet till filtrering och transformation längs vägen.
* **Loggning & Felhantering**: Kombinerad översiktslogg och detaljerad felsökningslogg samt stöd för nätverksfel och återförsök.

## **Specifikationer**

## **Implementationsplan**
