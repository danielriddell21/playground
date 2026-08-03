package mix

import (
	"bytes"
	"encoding/json"

	"io"
	"net"
	"net/http"
	"os/exec"
	"time"
)

func init() {
	set.Add("authz", "the same permission question answered three ways", authz)
}

var questions = []struct{ label, user, relation, object string }{
	{"alice owns readme", "alice", "viewer", "readme"},
	{"bob owns design", "bob", "viewer", "design"},
	{"carol edits design", "carol", "viewer", "design"},
	{"dave is in the platform team", "dave", "viewer", "readme"},
	{"dave cannot edit readme", "dave", "editor", "readme"},
	{"alice has nothing on design", "alice", "viewer", "design"},
}

func authz(s *Session) {
	seedAuthz(s)
	if s.Err() != nil {
		return
	}

	s.Note("=== melange: the schema was compiled into functions in this database")
	melange := map[string]bool{}
	for _, q := range questions {
		var allowed bool
		if err := s.PG().QueryRow(s.Ctx(),
			`select check_permission('user', $1, $2, 'document', $3) = 1`,
			q.user, q.relation, q.object).Scan(&allowed); err != nil {
			s.Note("  melange functions are missing, run: go run . melange compile")
			return
		}
		melange[q.label] = allowed
	}

	s.Note("=== openfga: the same model, answered by a separate service")
	fga, ok := askOpenFGA(s)
	if !ok {
		s.Note("  skipping the openfga column, no server available")
	}

	s.Note("=== postgres rls: no model at all, just policies on the table")
	rls := askRLS(s)

	var rows [][]any
	for _, q := range questions {
		row := []any{q.label, melange[q.label]}
		if ok {
			row = append(row, fga[q.label])
		} else {
			row = append(row, "-")
		}
		if v, seen := rls[q.label]; seen {
			row = append(row, v)
		} else {
			row = append(row, "n/a")
		}
		rows = append(rows, row)
	}
	s.Table([]string{"question", "melange", "openfga", "rls"}, rows)

	if ok {
		agree := true
		for _, q := range questions {
			if melange[q.label] != fga[q.label] {
				agree = false
			}
		}
		s.Note("melange and openfga agree on every question: %v", agree)
	}
	s.Note("rls only answers the viewer questions: a policy filters rows,")
	s.Note("it does not answer an arbitrary subject-relation-object question")
	s.Note("and it disagrees on carol and dave, because this policy only knows about owners")
	s.Note("editor and team membership would each need more policy, written by hand")

	s.Note("=== so")
	s.Table([]string{"", "openfga", "melange", "rls"}, [][]any{
		{"where the rules live", "its own service", "compiled into your db", "table policies"},
		{"where the tuples live", "openfga storage", "a view over your tables", "your tables"},
		{"can it be stale", "yes, you sync it", "no, same transaction", "no"},
		{"cost of a check", "a network call", "a sql function call", "free, it is the query"},
		{"filter a list", "list-objects call", "a where clause", "automatic"},
		{"who else can run it", "any language", "anything speaking sql", "only postgres"},
	})
}

func seedAuthz(s *Session) {
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

func askRLS(s *Session) map[string]bool {
	s.Exec(`drop table if exists rls_docs`)
	s.Exec(`create table rls_docs (id text primary key, owner_id text)`)
	s.Exec(`insert into rls_docs values ('readme','alice'), ('design','bob')`)
	for _, who := range []string{"alice", "bob", "carol", "dave"} {
		s.Exec(`do $$ begin
			if not exists (select from pg_roles where rolname = '` + who + `') then
				create role ` + who + `;
			end if;
		end $$`)
		s.Exec(`grant select on rls_docs to ` + who)
	}
	s.Exec(`alter table rls_docs enable row level security`)
	s.Exec(`drop policy if exists owner_reads on rls_docs`)
	s.Exec(`create policy owner_reads on rls_docs for select using (owner_id = current_user)`)

	out := map[string]bool{}
	for _, q := range questions {
		if q.relation != "viewer" {
			continue
		}
		// a superuser bypasses row level security, so become the user first
		s.Exec(`set role ` + q.user)
		var n int
		err := s.PG().QueryRow(s.Ctx(), `select count(*) from rls_docs where id = $1`, q.object).Scan(&n)
		s.Exec(`reset role`)
		if err != nil {
			s.Fail(err)
			return out
		}
		out[q.label] = n > 0
	}
	return out
}

func askOpenFGA(s *Session) (map[string]bool, bool) {
	addr := env("OPENFGA_ADDR", "127.0.0.1:8080")
	var server *exec.Cmd
	if !dialable(addr) {
		bin, err := exec.LookPath("openfga")
		if err != nil {
			return nil, false
		}
		server = exec.Command(bin, "run", "--datastore-engine", "memory")
		if err := server.Start(); err != nil {
			return nil, false
		}
		defer func() {
			server.Process.Kill()
			server.Wait()
		}()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && !dialable(addr) {
			time.Sleep(500 * time.Millisecond)
		}
		if !dialable(addr) {
			return nil, false
		}
	}

	post := func(path string, body, into any) bool {
		payload, _ := json.Marshal(body)
		resp, err := http.Post("http://"+addr+path, "application/json", bytes.NewReader(payload))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode/100 != 2 {
			return false
		}
		return into == nil || json.Unmarshal(raw, into) == nil
	}

	var store struct {
		ID string `json:"id"`
	}
	if !post("/stores", map[string]any{"name": "mix"}, &store) {
		return nil, false
	}

	var written struct {
		ID string `json:"authorization_model_id"`
	}
	if !post("/stores/"+store.ID+"/authorization-models", fgaModel(), &written) {
		return nil, false
	}

	keys := []map[string]string{
		{"user": "user:alice", "relation": "owner", "object": "document:readme"},
		{"user": "user:bob", "relation": "owner", "object": "document:design"},
		{"user": "user:carol", "relation": "editor", "object": "document:design"},
		{"user": "user:carol", "relation": "member", "object": "team:platform"},
		{"user": "user:dave", "relation": "member", "object": "team:platform"},
		{"user": "team:platform#member", "relation": "viewer", "object": "document:readme"},
	}
	if !post("/stores/"+store.ID+"/write", map[string]any{
		"authorization_model_id": written.ID,
		"writes":                 map[string]any{"tuple_keys": keys},
	}, nil) {
		return nil, false
	}

	out := map[string]bool{}
	for _, q := range questions {
		var res struct {
			Allowed bool `json:"allowed"`
		}
		if !post("/stores/"+store.ID+"/check", map[string]any{
			"authorization_model_id": written.ID,
			"tuple_key": map[string]string{
				"user": "user:" + q.user, "relation": q.relation, "object": "document:" + q.object},
		}, &res) {
			return nil, false
		}
		out[q.label] = res.Allowed
	}
	return out, true
}

func fgaModel() map[string]any {
	direct := func(types ...map[string]any) map[string]any {
		return map[string]any{"directly_related_user_types": types}
	}
	return map[string]any{
		"schema_version": "1.1",
		"type_definitions": []map[string]any{
			{"type": "user"},
			{"type": "team",
				"relations": map[string]any{"member": map[string]any{"this": map[string]any{}}},
				"metadata":  map[string]any{"relations": map[string]any{"member": direct(map[string]any{"type": "user"})}}},
			{"type": "document",
				"relations": map[string]any{
					"owner": map[string]any{"this": map[string]any{}},
					"editor": map[string]any{"union": map[string]any{"child": []map[string]any{
						{"this": map[string]any{}}, {"computedUserset": map[string]any{"relation": "owner"}}}}},
					"viewer": map[string]any{"union": map[string]any{"child": []map[string]any{
						{"this": map[string]any{}}, {"computedUserset": map[string]any{"relation": "editor"}}}}},
				},
				"metadata": map[string]any{"relations": map[string]any{
					"owner":  direct(map[string]any{"type": "user"}),
					"editor": direct(map[string]any{"type": "user"}, map[string]any{"type": "team", "relation": "member"}),
					"viewer": direct(map[string]any{"type": "user"}, map[string]any{"type": "team", "relation": "member"}),
				}}},
		},
	}
}

func dialable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
