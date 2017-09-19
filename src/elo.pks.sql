CREATE OR REPLACE PACKAGE ELO AUTHID CURRENT_USER
AS
  procedure run(piv_name varchar2);

  function script(
    fiv_table varchar2, 
    fiv_dblk varchar2, 
    fiv_name varchar2 default null, 
    fiv_target_schema varchar2 default 'ODS'
  );

END;
/