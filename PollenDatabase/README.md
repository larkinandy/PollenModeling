# PollenDatabase
PostGIS database for storing pollen metrics. Designed for pollen modeling at moderate resolution (e.g. neighborhood) in a small number (5-7) of core based statistical areas.

**Folder Structure** <br>
The pollen database folder contains the following components.
- **[Entity Relationship Diagram](https://github.com/larkinandy/PollenModeling/tree/main/PollenDatabase)** - database schema and strucutre <br>
- **[Data Dictionary](https://github.com/larkinandy/PollenModeling/tree/main/EnvironmentDatabase)** - table properties and column metadata in human readable form <br>
- **[CreateTables.txt](https://github.com/larkinandy/PollenModeling/tree/main/VisualizationTools)** - SQL commands used to create and update tables and constraints/checks <br>
- **[PollenSenseAPI.py](https://github.com/larkinandy/PollenModeling/tree/main/ModelDevelopment)** - custom class to query the Pollen Sense API<br>
- **[SQLAPI.py](https://github.com/larkinandy/PollenModeling/tree/main/ModelDevelopment)** - custom class to in query and update the pollen SQL database<br>
- **[PopulateDatabase.py](https://github.com/larkinandy/PollenModeling/tree/main/ModelDevelopment)** - script for query the pollen sense API and upserting the SQL database <br>

**TODO:**
- ER diagram
- add daily statistics table (site and city) and populate
- provision history
- add PPM column to hourly_metrics and populate


**External Links**
- **[PostGIS](https://postgis.net/)**
- **[Pollen Sense](https://pollensense.com/)**
- **[Core Based Statistical Areas](https://www.congress.gov/crs-product/IF12704)**
