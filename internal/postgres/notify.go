package postgres

import (
	"context"
	"time"
)

func init() {
	set.Add("notify", "listen and notify as a lightweight pub/sub", notify)
}

func notify(s *Session) {
	s.Exec(`listen chatter`)

	other, err := s.Pool().Acquire(s.Ctx())
	if err != nil {
		s.Fail(err)
		return
	}
	defer other.Release()

	s.Note("a second connection sends three messages")
	for _, msg := range []string{"hello", "from", "postgres"} {
		if _, err := other.Exec(s.Ctx(), `select pg_notify('chatter', $1)`, msg); err != nil {
			s.Fail(err)
			return
		}
	}

	s.Note("the listening connection receives them")
	for range 3 {
		ctx, cancel := context.WithTimeout(s.Ctx(), 3*time.Second)
		n, err := s.conn.Conn().WaitForNotification(ctx)
		cancel()
		if err != nil {
			s.Fail(err)
			return
		}
		s.Note("  channel=%s payload=%s pid=%d", n.Channel, n.Payload, n.PID)
	}

	s.Note("triggers can notify on data changes")
	s.Exec(`drop table if exists signups`)
	s.Exec(`create table signups (id serial primary key, email text)`)
	s.Exec(`create or replace function announce_signup() returns trigger as $$
		begin
			perform pg_notify('chatter', new.email);
			return new;
		end;
		$$ language plpgsql`)
	s.Exec(`create trigger signups_notify after insert on signups for each row execute function announce_signup()`)

	if _, err := other.Exec(s.Ctx(), `insert into signups (email) values ('ada@example.com')`); err != nil {
		s.Fail(err)
		return
	}

	ctx, cancel := context.WithTimeout(s.Ctx(), 3*time.Second)
	n, err := s.conn.Conn().WaitForNotification(ctx)
	cancel()
	if err != nil {
		s.Fail(err)
		return
	}
	s.Note("  channel=%s payload=%s", n.Channel, n.Payload)

	s.Exec(`unlisten chatter`)
}
