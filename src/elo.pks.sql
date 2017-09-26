CREATE OR REPLACE PACKAGE UTIL.ELO AUTHID CURRENT_USER
AS
  procedure run(i_name varchar2);

  procedure define(
    i_table varchar2, 
    i_dblk  varchar2, 
    i_name  varchar2 default null, 
    i_target_schema varchar2 default 'ODS'
  );

  function script(
    i_table varchar2, 
    i_dblk varchar2, 
    i_name varchar2 default null, 
    i_target_schema varchar2 default 'ODS'
  ) return varchar2;

END;