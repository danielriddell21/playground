package mix

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

func init() {
	set.Add("graph", "a graphql query answered by one postgres 19 graph query", graphDemo)
}

const question = `{
  person(name: "ada") {
    name
    follows {
      name
      follows {
        name
        city
      }
    }
  }
}`

func graphDemo(s *Session) {
	if !s.AtLeast(19) {
		return
	}
	seedGraph(s)
	if s.Err() != nil {
		return
	}

	s.Note("the same graphql question, asked twice")
	fmt.Println(question)

	s.Note("=== resolvers that walk one edge at a time")
	naive, err := buildSchema(s, false)
	if err != nil {
		s.Fail(err)
		return
	}
	s.ResetQueries()
	runQuery(s, naive)
	s.Note("postgres queries: %d", s.Queries())

	s.Note("=== one resolver that hands the whole walk to GRAPH_TABLE")
	graphed, err := buildSchema(s, true)
	if err != nil {
		s.Fail(err)
		return
	}
	s.ResetQueries()
	runQuery(s, graphed)
	s.Note("postgres queries: %d", s.Queries())

	s.Note("graphql nesting is a shape the client asks for")
	s.Note("the traversal is the database's problem, and postgres 19 can express it directly")
}

func seedGraph(s *Session) {
	s.Exec(`drop property graph if exists mix_social`)
	s.Exec(`drop table if exists mix_follows`)
	s.Exec(`drop table if exists mix_person`)
	s.Exec(`create table mix_person (id int primary key, name text, city text)`)
	s.Exec(`create table mix_follows (
		follower_id int references mix_person(id),
		followee_id int references mix_person(id)
	)`)
	s.Exec(`insert into mix_person values
		(1,'ada','london'), (2,'linus','helsinki'), (3,'grace','new york'),
		(4,'ken','murray hill'), (5,'barbara','philadelphia'), (6,'alan','wilmslow'),
		(7,'edsger','rotterdam'), (8,'donald','milwaukee'), (9,'tony','london'),
		(10,'niklaus','winterthur'), (11,'john','budapest'), (12,'maurice','dursley')`)
	s.Exec(`insert into mix_follows values
		(1,2), (1,3), (1,4), (1,5), (1,6),
		(2,7), (2,8), (3,8), (3,9), (4,10), (4,11), (5,12), (5,6), (6,9)`)
	s.Exec(`create property graph mix_social
		vertex tables (mix_person key (id) label person properties (name, city))
		edge tables (
			mix_follows key (follower_id, followee_id)
				source key (follower_id) references mix_person (id)
				destination key (followee_id) references mix_person (id)
				label follows
		)`)
}

func runQuery(s *Session, schema graphql.Schema) {
	result := graphql.Do(graphql.Params{Schema: schema, RequestString: question, Context: s.Ctx()})
	out := map[string]any{}
	if result.Data != nil {
		out["data"] = result.Data
	}
	if len(result.Errors) > 0 {
		msgs := make([]string, len(result.Errors))
		for i, e := range result.Errors {
			msgs[i] = e.Message
		}
		out["errors"] = msgs
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		s.Fail(err)
	}
}

func buildSchema(s *Session, useGraphTable bool) (graphql.Schema, error) {
	person := graphql.NewObject(graphql.ObjectConfig{
		Name: "Person",
		Fields: graphql.Fields{
			"name": &graphql.Field{Type: graphql.String},
			"city": &graphql.Field{Type: graphql.String},
		},
	})
	person.AddFieldConfig("follows", &graphql.Field{Type: graphql.NewList(person)})

	if !useGraphTable {
		person.AddFieldConfig("follows", &graphql.Field{
			Type: graphql.NewList(person),
			Resolve: func(p graphql.ResolveParams) (any, error) {
				src, _ := p.Source.(map[string]any)
				return oneHop(s, src["name"].(string))
			},
		})
	}

	query := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"person": &graphql.Field{
				Type: person,
				Args: graphql.FieldConfigArgument{
					"name": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					name := p.Args["name"].(string)
					if !useGraphTable {
						return lookup(s, name)
					}
					depth := followsDepth(p.Info.FieldASTs[0].SelectionSet)
					return walk(s, name, depth)
				},
			},
		},
	})

	return graphql.NewSchema(graphql.SchemaConfig{Query: query})
}

func followsDepth(set *ast.SelectionSet) int {
	if set == nil {
		return 0
	}
	best := 0
	for _, sel := range set.Selections {
		field, ok := sel.(*ast.Field)
		if !ok || field.Name.Value != "follows" {
			continue
		}
		if d := 1 + followsDepth(field.SelectionSet); d > best {
			best = d
		}
	}
	return best
}

func lookup(s *Session, name string) (map[string]any, error) {
	pool := s.PG()
	s.CountQuery()
	var city string
	err := pool.QueryRow(s.Ctx(), `select city from mix_person where name = $1`, name).Scan(&city)
	if err != nil {
		return nil, err
	}
	return map[string]any{"name": name, "city": city}, nil
}

func oneHop(s *Session, name string) ([]any, error) {
	pool := s.PG()
	s.CountQuery()
	rows, err := pool.Query(s.Ctx(), `
		select p.name, p.city
		from mix_person me
		join mix_follows f on f.follower_id = me.id
		join mix_person p on p.id = f.followee_id
		where me.name = $1
		order by p.name`, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []any
	for rows.Next() {
		var n, c string
		if err := rows.Scan(&n, &c); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"name": n, "city": c})
	}
	return out, rows.Err()
}

func walk(s *Session, name string, depth int) (map[string]any, error) {
	if depth == 0 {
		return lookup(s, name)
	}

	pattern := []string{}
	columns := []string{"a.name as root_name", "a.city as root_city"}
	for hop := 1; hop <= depth; hop++ {
		pattern = append(pattern, fmt.Sprintf("-[is follows]-> (h%d is person)", hop))
		columns = append(columns, fmt.Sprintf("h%d.name as n%d, h%d.city as c%d", hop, hop, hop, hop))
	}
	sql := fmt.Sprintf(`select * from graph_table (mix_social
		match (a is person where a.name = $1) %s
		columns (%s))`, strings.Join(pattern, " "), strings.Join(columns, ", "))

	pool := s.PG()
	s.CountQuery()
	rows, err := pool.Query(s.Ctx(), sql, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type node struct {
		name, city string
		kids       map[string]*node
		order      []string
	}
	tree := &node{name: name, kids: map[string]*node{}}
	seenRoot := false

	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		if !seenRoot {
			tree.city, _ = vals[1].(string)
			seenRoot = true
		}
		cur := tree
		for hop := range depth {
			n, _ := vals[2+hop*2].(string)
			c, _ := vals[2+hop*2+1].(string)
			if n == "" {
				break
			}
			next, seen := cur.kids[n]
			if !seen {
				next = &node{name: n, city: c, kids: map[string]*node{}}
				cur.kids[n] = next
				cur.order = append(cur.order, n)
			}
			cur = next
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var toMap func(n *node) map[string]any
	toMap = func(n *node) map[string]any {
		m := map[string]any{"name": n.name, "city": n.city}
		if len(n.order) > 0 {
			kids := make([]any, 0, len(n.order))
			for _, k := range n.order {
				kids = append(kids, toMap(n.kids[k]))
			}
			m["follows"] = kids
		}
		return m
	}
	if !seenRoot {
		return lookup(s, name)
	}
	return toMap(tree), nil
}
