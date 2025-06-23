package postgres

func init() {
	set.Add("transactions", "savepoints, isolation levels and deferred constraints", transactions)
}

func transactions(s *Session) {
	s.Exec(`drop table if exists ledger`)
	s.Exec(`create table ledger (id int primary key, note text)`)

	s.Note("savepoints roll back part of a transaction")
	s.Exec(`begin`)
	s.Exec(`insert into ledger values (1, 'kept')`)
	s.Exec(`savepoint sp1`)
	s.Exec(`insert into ledger values (2, 'discarded')`)
	s.Exec(`rollback to savepoint sp1`)
	s.Exec(`insert into ledger values (3, 'kept too')`)
	s.Exec(`commit`)
	s.Show(`select * from ledger order by id`)

	s.Note("isolation levels")
	s.Show(`select current_setting('default_transaction_isolation') as default_level`)
	s.Exec(`begin isolation level repeatable read`)
	s.Show(`select current_setting('transaction_isolation') as inside_transaction`)
	s.Exec(`commit`)

	s.Note("read only and deferrable transactions")
	s.Exec(`begin transaction read only`)
	s.Show(`select count(*) as rows_visible from ledger`)
	s.Exec(`commit`)

	s.Note("deferred constraints check at commit")
	s.Exec(`drop table if exists nodes`)
	s.Exec(`create table nodes (id int primary key, parent_id int references nodes(id) deferrable initially deferred)`)
	s.Exec(`begin`)
	s.Exec(`insert into nodes values (1, 2)`)
	s.Exec(`insert into nodes values (2, null)`)
	s.Exec(`commit`)
	s.Show(`select * from nodes order by id`)

	s.Note("transaction snapshot info")
	s.Show(`select txid_current() as tx, pg_current_xact_id_if_assigned() is not null as assigned`)
}
