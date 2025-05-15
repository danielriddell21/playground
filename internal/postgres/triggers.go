package postgres

func init() {
	set.Add("triggers", "plpgsql functions, triggers and audit trails", triggers)
}

func triggers(s *Session) {
	s.Exec(`drop table if exists accounts cascade`)
	s.Exec(`drop table if exists accounts_audit`)

	s.Exec(`create table accounts (id int primary key, owner text, balance numeric, updated_at timestamptz)`)
	s.Exec(`create table accounts_audit (id serial primary key, account_id int, action text, old_balance numeric, new_balance numeric, at timestamptz default now())`)

	s.Note("plain plpgsql function")
	s.Exec(`create or replace function fee(amount numeric) returns numeric as $$
		begin
			if amount > 100 then
				return round(amount * 0.01, 2);
			end if;
			return 1.00;
		end;
		$$ language plpgsql immutable`)
	s.Show(`select fee(50) as small, fee(1000) as large`)

	s.Note("row level trigger writing an audit row")
	s.Exec(`create or replace function audit_accounts() returns trigger as $$
		begin
			insert into accounts_audit (account_id, action, old_balance, new_balance)
			values (coalesce(new.id, old.id), tg_op, old.balance, new.balance);
			if tg_op <> 'DELETE' then
				new.updated_at := now();
				return new;
			end if;
			return old;
		end;
		$$ language plpgsql`)
	s.Exec(`create trigger accounts_audit_trg before insert or update or delete on accounts
		for each row execute function audit_accounts()`)

	s.Exec(`insert into accounts (id, owner, balance) values (1, 'ada', 100), (2, 'linus', 50)`)
	s.Exec(`update accounts set balance = balance - 25 where id = 1`)
	s.Exec(`delete from accounts where id = 2`)

	s.Show(`select account_id, action, old_balance, new_balance from accounts_audit order by id`)
	s.Show(`select id, owner, balance, updated_at is not null as stamped from accounts`)

	s.Note("statement level trigger")
	s.Exec(`create or replace function log_statement() returns trigger as $$
		begin
			raise notice 'statement % on %', tg_op, tg_table_name;
			return null;
		end;
		$$ language plpgsql`)
	s.Exec(`create trigger accounts_stmt_trg after update on accounts
		for each statement execute function log_statement()`)
	s.Exec(`update accounts set balance = balance + 1`)

	s.Note("returning a set from a function")
	s.Exec(`create or replace function top_accounts(min_balance numeric)
		returns table (owner text, balance numeric) as $$
		begin
			return query select a.owner, a.balance from accounts a where a.balance >= min_balance;
		end;
		$$ language plpgsql stable`)
	s.Show(`select * from top_accounts(10)`)
}
