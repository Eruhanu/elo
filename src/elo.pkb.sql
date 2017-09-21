CREATE OR REPLACE PACKAGE BODY UTIL.ELO
AS

  gv_job_module   VARCHAR2(50)  := 'ELO';                 -- Job Module Name : Extract Load package
  gv_pck          VARCHAR2(50)  := 'ELO';                 -- PLSQL Package Name
  gv_job_owner    VARCHAR2(50)  := 'UTIL';                -- Owner of the Job
  gv_proc         VARCHAR2(100);                          -- Procedure Name
  gv_delim        VARCHAR2(10)  := ' : ';                 -- Delimiter Used In Logging

  gv_sql_errm     VARCHAR2(4000);                         -- SQL Error Message
  gv_sql_errc     NUMBER;                                 -- SQL Error Code
  gv_sql          LONG := '';
  gv_date_format  varchar2(30) := 'yyyy.mm.dd hh24:mi:ss';

  function fun_get_delta_col_type(
    iv_db_link varchar2,
    iv_table   varchar2,
    iv_column  varchar2) return varchar2;

  procedure prc_update_last_delta(
    iv_name            varchar2,
    iv_table           varchar2,
    iv_delta_col       varchar2,
    iv_delta_col_type  varchar2
  );

  procedure run(iv_name varchar2)
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
    pl.logger := util.logtype.init(gv_pck||'.'||gv_proc);


    select
      db_link, source, target, filter, source_hint, target_hint, v_delta_column, last_delta
    into
      v_db_link, v_source, v_target, v_filter, v_source_hint, v_target_hint, v_delta_column, v_last_delta
    from
      util.ELO_TABLES
    where
      name = iv_name;

    if trim(v_target_hint) is not null and instr(v_target_hint,'/*+') = 0
    then
      v_target_hint := '/*+ '||v_target_hint||' */';
    end if;

    if trim(v_source_hint) is not null and instr(v_source_hint,'/*+') = 0
    then
      v_source_hint := '/*+ '||v_source_hint||' */';
    end if;

    IF v_delta_column IS NOT NULL THEN
      v_delta_data_type := fun_get_delta_col_type(v_db_link, v_source, v_delta_column);
    END IF;	
    	
    if v_filter is not null or v_delta_column is not null then
      v_filter := 'WHERE ' || v_filter;
      if v_delta_column is not null then
        v_filter := v_filter || ' AND '||v_delta_column||'>'||
        case v_delta_data_type
          when 'DATE'     then pl.date_string(to_date(v_last_delta,gv_date_format))
          when 'NUMBER'   then to_number(v_last_delta)
          when 'CHAR'     then v_last_delta
          when 'VARCHAR'  then v_last_delta
          else v_last_delta
        end;
      end if;
    end if;

    select
      LISTAGG(source_col, ', ') WITHIN GROUP (ORDER BY source_col) source_cols,
      LISTAGG(target_col, ', ') WITHIN GROUP (ORDER BY source_col) target_cols
    into v_source_cols, v_target_cols
    from util.ELO_COLUMNS
    where name = iv_name;

    gv_sql := '
      INSERT '||v_target_hint||' INTO '|| v_target ||'
      ('||v_target_cols|| ')
      SELECT '||v_source_hint||'
      '||v_source_cols||'
      FROM
        '||v_source||'@'||v_db_link||'
      '||v_filter||'';

    execute immediate 'truncate table ' || v_target;

    execute immediate gv_sql;
    commit;

    pl.logger.success(SQL%ROWCOUNT || ' : inserted', gv_sql);

    if v_delta_column is not null then
      prc_update_last_delta(
        iv_name  => iv_name,
        iv_table => v_target,
        iv_delta_col => v_delta_column,
        iv_delta_col_type => v_delta_data_type
      );
    end if;


  exception
    when others then
      gv_sql_errc := SQLCODE;
      gv_sql_errm := SQLERRM;
      pl.logger.error(gv_sql_errc||' : '||gv_sql_errm, gv_sql);
      raise_application_error(gv_sql_errc, gv_sql_errm);
  end;

  function fun_get_delta_col_type(iv_db_link varchar2, iv_table varchar2, iv_column varchar2) return varchar2
  is
    v_owner       varchar2(30) := substr(iv_table,1,instr(iv_table, '.')-1);
    v_table_name  varchar2(30) := substr(iv_table,instr(iv_table, '.')+1);
    v_col_type    varchar2(50);
  begin

    gv_sql:= '
      select data_type from all_tab_cols@'||iv_db_link||'
      where owner = '''||v_owner||''' and table_name='''||v_table_name||''' and column_name = '''|| iv_column||'''
    ';

    execute immediate gv_sql into v_col_type;

    return gv_sql;

  end;

  procedure prc_update_last_delta(
    iv_name            varchar2,
    iv_table           varchar2,
    iv_delta_col       varchar2,
    iv_delta_col_type  varchar2
  )
  is
    v_last_delta varchar2(1000);
  begin

    if iv_delta_col_type = 'DATE' then
      gv_sql := 'to_char(max('||iv_delta_col||'),'''||gv_date_format||''')';
    else
      gv_sql := 'max('||iv_delta_col||')';
    end if;

    gv_sql := 'select /*+ parallel(16) */ '||gv_sql||' from '||iv_table;

    execute immediate gv_sql into v_last_delta;

    gv_sql := '
      update ELO_TABLES set last_delta = '''||v_last_delta||'''
      where name = '''||iv_name||'''
    ';

    execute immediate gv_sql;

    commit;
  end;


  procedure define(
    iv_table varchar2, 
    iv_dblk  varchar2, 
    iv_name  varchar2 default null, 
    iv_target_schema varchar2 default 'ODS'
  )
  is 
    table_is_null     exception;
    db_link_is_null   exception;
    
    pragma exception_init(table_is_null,   -20170);
    pragma exception_init(db_link_is_null, -20171);

    v_script  long := '';
    v_columns long := '';

    type source_cursor_type is ref cursor;
    c source_cursor_type;

    v_column_name  varchar2(100);
    v_data_type    varchar2(100);
    v_data_length  number;
  begin

    if iv_table is null then raise table_is_null; end if;
    
    if iv_target_schema is null then raise db_link_is_null; end if;


    v_script := '
      create table '||iv_target_schema||'.'||substr(iv_table, instr(iv_target_schema,'.'))||' 
      (
        $COLUMNS
      )
    ';

    gv_sql := 'select column_name, data_type, data_length from all_tab_cols@'||iv_dblk|| '
      where owner||''.''||table_name = '''||upper(iv_table)||''' and
      hidden_column = ''NO''
    ';
    open c for gv_sql;

    loop
      
      fetch c into v_column_name, v_data_type, v_data_length;
      exit when c%notfound;

      if v_data_type in ('CHAR','VARCHAR','VARCHAR2','NUMBER') then
        v_columns := v_columns || v_column_name||' '||v_data_type||'('||v_data_length||'),'||chr(10);
      else
        v_columns := v_columns || v_column_name||' '||v_data_type||','||chr(10);
      end if;
    end loop;

    v_columns := rtrim(v_columns, ','||chr(10));  

    v_script := replace(v_script,'$COLUMNS',v_columns) ||chr(10)||chr(10);

    execute immediate v_script;
    pl.logger.success('Table created', v_script);

    v_script := 'INSERT INTO ELO_TABLES (
      NAME,        
      DB_LINK,     
      SOURCE,    
      TARGET      
    ) VALUES (
      '''||nvl(iv_name,iv_table)||''',
      '''||iv_dblk||''',
      '''||iv_table||''',
      '''||iv_target_schema||'.'||substr(iv_table, instr(iv_target_schema,'.'))||'''
    )';

    execute immediate v_script;
    pl.logger.success('Table defined in elo_tables', v_script);


    open c for gv_sql;

    loop
      
      fetch c into v_column_name, v_data_type, v_data_length;
      exit when c%notfound;

      v_script := 'INSERT INTO ELO_COLUMNS (
        NAME,
        SOURCE_COL,
        TARGET_COL
      ) VALUES (
        '''||nvl(iv_name, iv_table)||''',
        '''||v_column_name||''',
        '''||v_column_name||'''    
      )';
    
      execute immediate v_script;
      pl.logger.success('Column defined in elo_columns', v_script);

    end loop;

    commit;

  exception
    when others then
      gv_sql_errc := SQLCODE;
      gv_sql_errm := SQLERRM;
      pl.logger.error(gv_sql_errc||' : '||gv_sql_errm, gv_sql);
      raise_application_error(gv_sql_errc, gv_sql_errm);
  end;

  function script(
    iv_table varchar2, 
    iv_dblk  varchar2, 
    iv_name  varchar2 default null, 
    iv_target_schema varchar2 default 'ODS'
  ) return varchar2
  is 
    table_is_null     exception;
    db_link_is_null   exception;
    
    pragma exception_init(table_is_null,   -20170);
    pragma exception_init(db_link_is_null, -20171);

    v_script  long := '';
    v_columns long := '';

    type source_cursor_type is ref cursor;
    c source_cursor_type;

     v_column_name  varchar2(100);
     v_data_type    varchar2(100);
     v_data_length  number;
  begin

    if iv_table is null then raise table_is_null; end if;
    
    if iv_target_schema is null then raise db_link_is_null; end if;


    v_script := '
      create table '||iv_target_schema||'.'||substr(iv_table, instr(iv_target_schema,'.'))||' 
      (
        $COLUMNS
      );
    ';

    gv_sql := 'select column_name, data_type, data_length from all_tab_cols@'||iv_dblk|| '
      where owner||''.''||table_name = '''||upper(iv_table)||''' and
      hidden_column = ''NO''
    ';
    open c for gv_sql;

    loop
      
      fetch c into v_column_name, v_data_type, v_data_length;
      exit when c%notfound;

      if v_data_type in ('CHAR','VARCHAR','VARCHAR2','NUMBER') then
        v_columns := v_columns || v_column_name||' '||v_data_type||'('||v_data_length||'),'||chr(10);
      else
        v_columns := v_columns || v_column_name||' '||v_data_type||','||chr(10);
      end if;
    end loop;

    v_columns := rtrim(v_columns, ','||chr(10));  

    v_script := replace(v_script,'$COLUMNS',v_columns) ||chr(10)||chr(10);

    v_script := v_script || 'INSERT INTO ELO_TABLES (
      NAME,        
      DB_LINK,     
      SOURCE,    
      TARGET      
    ) VALUES (
      '''||nvl(iv_name,iv_table)||''',
      '''||iv_dblk||''',
      '''||iv_table||''',
      '''||iv_target_schema||'.'||substr(iv_table, instr(iv_target_schema,'.'))||'''
    );'||chr(10)||chr(10);


    open c for gv_sql;

    loop
      
      fetch c into v_column_name, v_data_type, v_data_length;
      exit when c%notfound;

      v_script := v_script || 'INSERT INTO ELO_COLUMNS (
        NAME,
        SOURCE_COL,
        TARGET_COL
      ) VALUES (
        '''||nvl(iv_name, iv_table)||''',
        '''||v_column_name||''',
        '''||v_column_name||'''    
      );'||chr(10)||chr(10);
    
    end loop;

    v_script := v_script || ' commit;';

    return v_script;

  end;



END;