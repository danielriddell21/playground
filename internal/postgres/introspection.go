package postgres

func init() {
	set.Add("introspection", "catalogs, system columns, settings and activity views", introspection)
}

func introspection(s *Session) {
	s.Note("server identity")
	s.Show(`select version() as version`)
	s.Show(`select current_database() as database, current_user as user, current_schema() as schema, inet_server_port() as port`)

	s.Note("settings worth knowing")
	s.Show(`select name, setting, unit from pg_settings
		where name in ('shared_buffers','work_mem','max_connections','wal_level','fsync','statement_timeout')
		order by name`)

	s.Note("changing a setting for this session")
	s.Exec(`set statement_timeout = '5s'`)
	s.Show(`select current_setting('statement_timeout') as statement_timeout`)
	s.Exec(`reset statement_timeout`)

	s.Note("tables and their sizes")
	s.Exec(`drop table if exists widgets`)
	s.Exec(`create table widgets (id serial primary key, name text)`)
	s.Exec(`insert into widgets (name) select 'w' || g from generate_series(1, 1000) g`)
	s.Show(`select tablename, pg_size_pretty(pg_total_relation_size(('public.' || tablename)::regclass)) as size
		from pg_tables where schemaname = 'public' order by pg_total_relation_size(('public.' || tablename)::regclass) desc limit 5`)

	s.Note("columns of a table")
	s.Show(`select column_name, data_type, is_nullable, column_default
		from information_schema.columns where table_name = 'widgets' order by ordinal_position`)

	s.Note("hidden system columns")
	s.Show(`select ctid, xmin::text, id, name from widgets limit 3`)

	s.Note("connections and activity")
	s.Show(`select datname, usename, state, count(*) from pg_stat_activity group by 1,2,3 order by 4 desc limit 5`)

	s.Note("database stats")
	s.Show(`select numbackends, xact_commit, xact_rollback, blks_hit, blks_read
		from pg_stat_database where datname = current_database()`)

	s.Note("vacuum and analyze")
	s.Exec(`vacuum analyze widgets`)
	s.Show(`select relname, last_vacuum is not null as vacuumed, last_analyze is not null as analyzed, n_dead_tup
		from pg_stat_user_tables where relname = 'widgets'`)

	s.Note("what this server can do")
	s.Show(`select count(*) as available_extensions from pg_available_extensions`)
	s.Show(`select count(*) as installed_functions from pg_proc where pronamespace = 'pg_catalog'::regnamespace`)
}
