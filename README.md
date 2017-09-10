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