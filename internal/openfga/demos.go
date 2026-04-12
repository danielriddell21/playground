package openfga

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
)

func init() {
	set.Add("model", "write an authorization model and some relationships", model)
	set.Add("check", "ask whether a user may do a thing, and who else may", check)
}

func (s *Session) post(path string, body any, into any) bool {
	if s.err != nil {
		return false
	}
	payload, err := json.Marshal(body)
	if err != nil {
		s.Fail(err)
		return false
	}
	resp, err := http.Post("http://"+s.addr+path, "application/json", bytes.NewReader(payload))
	if err != nil {
		s.Fail(err)
		return false
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		s.Fail(fmt.Errorf("%s -> %s: %s", path, resp.Status, string(raw)))
		return false
	}
	if into != nil {
		if err := json.Unmarshal(raw, into); err != nil {
			s.Fail(err)
			return false
		}
	}
	return true
}

func (s *Session) setup() bool {
	var store struct {
		ID string `json:"id"`
	}
	if !s.post("/stores", map[string]any{"name": "playground"}, &store) {
		return false
	}
	s.store = store.ID

	authModel := map[string]any{
		"schema_version": "1.1",
		"type_definitions": []map[string]any{
			{"type": "user"},
			{"type": "team", "relations": map[string]any{"member": map[string]any{"this": map[string]any{}}},
				"metadata": map[string]any{"relations": map[string]any{
					"member": map[string]any{"directly_related_user_types": []map[string]any{{"type": "user"}}}}}},
			{"type": "document",
				"relations": map[string]any{
					"owner": map[string]any{"this": map[string]any{}},
					"editor": map[string]any{"union": map[string]any{"child": []map[string]any{
						{"this": map[string]any{}},
						{"computedUserset": map[string]any{"relation": "owner"}}}}},
					"viewer": map[string]any{"union": map[string]any{"child": []map[string]any{
						{"this": map[string]any{}},
						{"computedUserset": map[string]any{"relation": "editor"}}}}},
				},
				"metadata": map[string]any{"relations": map[string]any{
					"owner":  map[string]any{"directly_related_user_types": []map[string]any{{"type": "user"}}},
					"editor": map[string]any{"directly_related_user_types": []map[string]any{{"type": "user"}, {"type": "team", "relation": "member"}}},
					"viewer": map[string]any{"directly_related_user_types": []map[string]any{{"type": "user"}, {"type": "team", "relation": "member"}}},
				}}},
		},
	}

	var written struct {
		ID string `json:"authorization_model_id"`
	}
	if !s.post("/stores/"+s.store+"/authorization-models", authModel, &written) {
		return false
	}
	s.model = written.ID
	return true
}

var tuples = []struct{ user, relation, object string }{
	{"user:alice", "owner", "document:readme"},
	{"user:bob", "owner", "document:design"},
	{"user:carol", "editor", "document:design"},
	{"user:carol", "member", "team:platform"},
	{"user:dave", "member", "team:platform"},
	{"team:platform#member", "viewer", "document:readme"},
}

func (s *Session) writeTuples() bool {
	var keys []map[string]string
	for _, t := range tuples {
		keys = append(keys, map[string]string{"user": t.user, "relation": t.relation, "object": t.object})
	}
	return s.post("/stores/"+s.store+"/write", map[string]any{
		"authorization_model_id": s.model,
		"writes":                 map[string]any{"tuple_keys": keys},
	}, nil)
}

func model(s *Session) {
	if !s.setup() {
		return
	}
	s.Note("a store and an authorization model, both created over http")
	s.Table([]string{"store id", "model id"}, [][]any{{s.store, s.model}})

	s.Note("the model is types and relations, the same shape melange compiles")
	s.Table([]string{"type", "relations"}, [][]any{
		{"user", "(none)"},
		{"team", "member"},
		{"document", "owner, editor = this or owner, viewer = this or editor"},
	})

	if !s.writeTuples() {
		return
	}
	s.Note("relationships are written as tuples, held by openfga itself")
	var rows [][]any
	for _, t := range tuples {
		rows = append(rows, []any{t.user, t.relation, t.object})
	}
	s.Table([]string{"user", "relation", "object"}, rows)
	s.Note("this is the part melange does differently: here the tuples live in openfga,")
	s.Note("not in the tables your application already writes to")
}

func check(s *Session) {
	if !s.setup() || !s.writeTuples() {
		return
	}

	questions := []struct{ label, user, relation, object string }{
		{"alice owns readme", "user:alice", "viewer", "document:readme"},
		{"bob owns design", "user:bob", "viewer", "document:design"},
		{"carol edits design", "user:carol", "viewer", "document:design"},
		{"dave is in the platform team", "user:dave", "viewer", "document:readme"},
		{"dave cannot edit readme", "user:dave", "editor", "document:readme"},
		{"alice has nothing on design", "user:alice", "viewer", "document:design"},
	}

	var rows [][]any
	for _, q := range questions {
		var out struct {
			Allowed bool `json:"allowed"`
		}
		if !s.post("/stores/"+s.store+"/check", map[string]any{
			"authorization_model_id": s.model,
			"tuple_key":              map[string]string{"user": q.user, "relation": q.relation, "object": q.object},
		}, &out) {
			return
		}
		rows = append(rows, []any{q.label, out.Allowed})
	}
	s.Table([]string{"question", "allowed"}, rows)

	s.Note("list-objects turns the question round: what can dave see")
	var listed struct {
		Objects []string `json:"objects"`
	}
	if !s.post("/stores/"+s.store+"/list-objects", map[string]any{
		"authorization_model_id": s.model,
		"type":                   "document",
		"relation":               "viewer",
		"user":                   "user:dave",
	}, &listed) {
		return
	}
	sort.Strings(listed.Objects)
	s.Table([]string{"documents dave can view"}, [][]any{{listed.Objects}})

	s.Note("every one of those was a network call to a separate service")
	s.Note("which is the trade: one place for authorization, one more thing to run and keep in sync")
}
