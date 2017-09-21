CREATE OR REPLACE PACKAGE UTIL.ELO AUTHID CURRENT_USER
AS
  procedure run(miv_name varchar2);

  procedure define(
    miv_table varchar2, 
    miv_dblk  varchar2, 
    miv_name  varchar2 default null, 
    miv_target_schema varchar2 default 'ODS'
  );

  function script(
    miv_table varchar2, 
    miv_dblk varchar2, 
    miv_name varchar2 default null, 
    miv_target_schema varchar2 default 'ODS'
  ) return varchar2;

END;