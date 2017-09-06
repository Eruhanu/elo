CREATE OR REPLACE PACKAGE BODY ELO 
AS

  gv_job_module   VARCHAR2(50)  := 'ELO';                 -- Job Module Name : Extract/Load package
  gv_pck          VARCHAR2(50)  := 'ELO';                 -- PLSQL Package Name
  gv_job_owner    VARCHAR2(50)  := 'UTIL';                -- Owner of the Job
  gv_proc         VARCHAR2(100);                          -- Procedure Name
  gv_delim        VARCHAR2(10)  := ' : ';                 -- Delimiter Used In Logging

  gv_sql_errm     VARCHAR2(4000);                         -- SQL Error Message
  gv_sql_errc     NUMBER;                                 -- SQL Error Code
  gv_dyn_task     LONG := '';
  gv_date_format  varchar2(20) := 'yyyy.mm.dd hh24:mi:ss';

  function fun_get_delta_col_type(
    fiv_db_link varchar2, 
    fiv_table   varchar2, 
    fiv_column  varchar2) return varchar2;

  procedure prc_update_last_delta(
    piv_name            varchar2, 
    piv_table           varchar2, 
    piv_delta_col       varchar2,
    piv_delta_col_type  varchar2
  );

  procedure run(piv_name varchar2)
  is
    v_db_link         varchar2(60);
    v_source          varchar2(100);
    v_target          varchar2(100);
    v_filter          varchar2(4000);
    v_source_hint     varchar2(4000);  
    v_target_hint     varchar2(4000);
    v_delta_column    varchar2(50);
    v_last_delta      varchar2(1000);
    v_source_cols     long;
    v_target_cols     long;
    v_delta_data_type varchar2(50);
  begin

    gv_proc := 'RUN';
    
    -- Initialize Log Variables
    plib.o_log := log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    select 
      db_link, source, target, filter, source_hint, target_hint, v_delta_column, last_delta
    into 
      v_db_link, v_source, v_target, v_filter, v_source_hint, v_target_hint, v_delta_column, v_last_delta
    from 
      util.ELO_TABLES
    where
      name = piv_name; 

    if trim(v_target_hint) is not null and instr(v_target_hint,'/*+') = 0
    then
      v_target_hint := '/*+'||v_target_hint||'*/';
    end if;  

    if trim(v_source_hint) is not null and instr(v_source_hint,'/*+') = 0
    then
      v_source_hint := '/*+'||v_source_hint||'*/';
    end if;

    v_delta_col_type := fun_get_delta_col_type(v_db_link, v_source, v_delta_column);

    if v_filter is not null or v_delta_column is not null then
      v_filter := 'WHERE ' || v_filter;
      if v_delta_column is not null then
        v_filter := v_filter || ' AND '||v_delta_column||'>'||
        case v_delta_col_type 
          when 'DATE'     then plib.date_string(to_date(v_last_delta,gv_date_format))
          when 'NUMBER'   then to_number(v_last_delta)
          when 'CHAR'     then v_last_delta
          when 'VARCHAR'  then v_last_delta
          else v_last_delta
        end;
      end if;
    end if;

    select 
      LISTAGG(source_col, ', ') WITHIN GROUP (ORDER BY source_col) source_cols,
      LISTAGG(target_col, ', ') WITHIN GROUP (ORDER BY target_col) target_cols
    into v_source_cols, v_target_cols
    from ELO_COLUMNS
    where name = piv_name;

    gv_dyn_task := '
      INSERT '||v_target_hint||' INTO '|| v_target ||'  
      ('||v_target_cols|| ')
      SELECT '||v_source_hint||'
      '||v_source_cols||'
      FROM
        '||v_source||'@'||v_db_link||'
      '||v_filter||'';

    execute immediate 'truncate table ' || v_target;

    execute immediate gv_dyn_task;
    commit;

    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);

    if v_delta_column is not null then
      prc_update_last_delta(
        piv_name  => piv_name,     
        piv_table => v_table_name,        
        piv_delta_col => v_delta_column,    
        piv_delta_col_type => v_delta_col_type
      );
    end if;


    exception
      when others then
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, NULL, NULL, gv_dyn_task);
        raise_application_error(gv_sql_errc, gv_sql_errm);
  end;

  function fun_get_delta_col_type(fiv_db_link varchar2, fiv_table varchar2, fiv_column varchar2) return varchar2
  is
    v_owner       varchar2(30) := substr(fiv_table,1,instr(fiv_table'.')-1);
    v_table_name  varchar2(30) := substr(fiv_table,instr(fiv_table'.')+1);
    v_col_type    varchar2(50);
  begin

    gv_dyn_task:= '
      select data_type from all_tab_cols@'||fiv_db_link||'
      where owner = '||v_owner||' and table_name='||v_table_name||'
    ';

    execute immediate gv_dyn_task into v_col_type;

    return gv_dyn_task;

  end;

  procedure prc_update_last_delta(
    piv_name            varchar2, 
    piv_table           varchar2, 
    piv_delta_col       varchar2,
    piv_delta_col_type  varchar2
  )
  is
    v_last_delta varchar2(1000);
  begin

    if piv_delta_col_type = 'DATE' then
      gv_dyn_task := 'to_char(max('||piv_delta_col||'),'''||gv_date_format||''')';
    else
      gv_dyn_task := 'max('||piv_delta_col||')';
    end if;

    gv_dyn_task := 'select /*+ parallel(16) */ '||gv_dyn_task||' from '||piv_table;
    
    execute immediate gv_dyn_task into v_last_delta;

    gv_dyn_task := '
      update ELO_TABLES set last_delta = '''||v_last_delta||'''
      where name = '''||piv_name||'''
    ';

    execute immediate gv_dyn_task;

    commit;
  end;

END;
/