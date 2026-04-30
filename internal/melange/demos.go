package melange

import (
	"os"
	"strings"
)

func init() {
	set.Add("compile", "turn an .fga schema into sql functions in your own database", compile)
	set.Add("check", "answer permission questions with one sql call", check)
	set.Add("explain", "ask why a check came back the way it did", explain)
}

func showSchema(s *Session) {
	body, err := os.ReadFile(repoRoot() + "/" + schemaPath)
	if err != nil {
		s.Fail(err)
		return
	}
	s.Note("the schema is openfga, not sql")
	for _, line := range strings.Split(strings.TrimRight(string(body), "\n"), "\n") {
		s.Note("  %s", line)
	}
}

func compile(s *Session) {
	showSchema(s)

	s.Note("melange validates it with the openfga parser")
	if out, ok := s.CLI("validate", "--schema", schemaPath); ok {
		s.Echo(out)
	} else {
		return
	}

	s.Note("then compiles it into functions inside postgres")
	if out, ok := s.CLI("migrate", "--db", s.DSN(), "--schema", schemaPath); ok {
		s.Echo(out)
	} else {
		return
	}

	s.Note("one function per type and relation, plus the generic entry points")
	s.Show(`select proname as function
		from pg_proc p join pg_namespace n on n.oid = p.pronamespace
		where n.nspname = 'public' and proname like 'check_%'
		order by proname`)

	s.Note("no separate authorization service is running, this is all in the database")
}

func seed(s *Session) {
	s.Exec(`drop view if exists melange_tuples`)
	s.Exec(`drop table if exists doc_team_access, doc_editors, team_members, docs cascade`)
	s.Exec(`create table docs (id text primary key, title text, owner_id text)`)
	s.Exec(`create table team_members (team_id text, user_id text)`)
	s.Exec(`create table doc_editors (doc_id text, user_id text)`)
	s.Exec(`create table doc_team_access (doc_id text, team_id text)`)

	s.Exec(`insert into docs values ('readme','README','alice'), ('design','Design Doc','bob')`)
	s.Exec(`insert into team_members values ('platform','carol'), ('platform','dave')`)
	s.Exec(`insert into doc_editors values ('design','carol')`)
	s.Exec(`insert into doc_team_access values ('readme','platform')`)

	s.Exec(`create or replace view melange_tuples as
		select 'document' as object_type, id as object_id, 'owner' as relation,
		       'user' as subject_type, owner_id as subject_id from docs
		union all
		select 'document', doc_id, 'editor', 'user', user_id from doc_editors
		union all
		select 'team', team_id, 'member', 'user', user_id from team_members
		union all
		select 'document', doc_id, 'viewer', 'team', team_id || '#member' from doc_team_access`)
}

func check(s *Session) {
	seed(s)

	s.Note("melange_tuples is a view over the tables you already have")
	s.Note("there is no second copy of the data to keep in sync")
	s.Show(`select object_type, object_id, relation, subject_type, subject_id
		from melange_tuples order by object_type, object_id, relation, subject_id`)

	s.Note("now ask questions")
	s.Show(`select q, check_permission('user', subj, rel, 'document', obj) = 1 as allowed
		from (values
			('alice owns readme',              'alice', 'viewer', 'readme'),
			('bob owns design',                'bob',   'viewer', 'design'),
			('carol edits design',             'carol', 'viewer', 'design'),
			('dave is in the platform team',   'dave',  'viewer', 'readme'),
			('dave cannot edit readme',        'dave',  'editor', 'readme'),
			('alice has nothing on design',    'alice', 'viewer', 'design')
		) as t(q, subj, rel, obj)`)

	s.Note("dave was never granted anything on readme directly")
	s.Note("the team has viewer, dave is a member, and the compiled sql walked that for you")

	s.Note("checks compose with ordinary sql, so filtering a list is one query")
	s.Show(`select d.id, d.title
		from docs d
		where check_permission('user', 'dave', 'viewer', 'document', d.id) = 1
		order by d.id`)
}

func explain(s *Session) {
	seed(s)

	s.Note("a check tells you yes or no")
	s.Show(`select check_permission('user','dave','viewer','document','readme') = 1 as dave_can_view_readme`)

	s.Note("explain tells you how it got there")
	if out, ok := s.CLI("explain", "--db", s.DSN(), "user:dave", "viewer", "document:readme"); ok {
		s.Echo(out)
	}

	s.Note("and the same for a check that fails")
	if out, ok := s.CLI("explain", "--db", s.DSN(), "user:dave", "editor", "document:readme"); ok {
		s.Echo(out)
	}
}
