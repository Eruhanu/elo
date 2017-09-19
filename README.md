<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.


Simple extract-load utility for **oracle** database. ELO is DB-Link only so you
can only use it for oracle -> oracle extractions. It is dead-simple and super fast
to define extraction rule for a table.

### Dependencies
  
  **ELO** uses [PL](https://github.com/bluecolor/pl) for logging. 
  **PL** is a small utility and logging library for oracle. 

### Installation

  * Make sure you have installed [PL](https://github.com/bluecolor/pl)

  * Change the current schema to util

    ```sql
    alter session set current_schema = util;
    ```
  
  * Run the contents of [init.sql](src/init.sql)

  * Run the contents of [elo.pks.sql](src/elo.pks.sql)

  * Run the contents of [elo.pkb.sql](src/elo.pkb.sql)

### Tables
  
  **ELO_TABLES**
  
  * `name` unique name for the extraction of the table
  * `db_link` db link to use for the extraction  
  * `source` source table including schema `eg. SRC_SCHEMA.TABLE_NAME`
  * `target` target table including schema `eg. TRG_SCHEMA.TABLE_NAME`
  * `filter` filter for the source data
  * `source_hint` select hint for the source
  * `target_hint` insert hint for the target
  * `delta_column` column to check if data is extracted using delta method.  
  * `last_delta` last extracted value of the delta column

  **ELO_COLUMNS**

  * `name` unique name for the extraction of the table. same name with `ELO_TABLES`
  * `source_col` source column or expression to extract.    
  * `target_col` target column to load data.


### Running

  Just call `elo.run` with a name parameter. example:

  ```sql
    elo.run('NAME_OF_EXT_DEF');
  ```

### Logs

  You can see the logs by issuing a select like;

  ```sql
    select * from util.logs order by 3 desc;
  ```