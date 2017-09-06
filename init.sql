CREATE TABLE ELO_TABLES
(
  name          varchar2(100), 
  db_link       varchar2(60),
  source        varchar2(100),
  target        varchar2(100),
  filter        varchar2(4000),
  source_hint   varchar2(4000),
  target_hint   varchar2(4000),
  delta_column  varchar2(50),
  last_delta    varchar2(1000)
)
NOLOGGING;

CREATE OR REPLACE TRIGGER TRG_ELO_TABLES_UCASE
BEFORE INSERT ON ELO_TABLES FOR EACH ROW
DECLARE
BEGIN
  :new.name    := upper(:new.name);
  :new.db_link := upper(:new.db_link);
  :new.source  := :new.source;
  :new.target  := upper(:new.target);
END;
/


drop table ELO_COLUMNS;
CREATE TABLE ELO_COLUMNS
(
  name          varchar2(100),
  source_col    varchar2(1000),
  target_col    varchar2(50)
)
NOLOGGING;

CREATE OR REPLACE TRIGGER TRG_ELO_COLUMNS_UCASE
BEFORE INSERT ON ELO_COLUMNS FOR EACH ROW
DECLARE
BEGIN
  :new.name       := upper(:new.name);
  :new.source_col := :new.source_col;
  :new.target_col := upper(:new.target_col);
END;
/