package graphql

import (
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/graphql-go/graphql"
)

type author struct {
	ID    string
	Name  string
	City  string
	Books []string
}

type book struct {
	ID       string
	Title    string
	Year     int
	AuthorID string
	Tags     []string
}

var authors = map[string]*author{
	"a1": {ID: "a1", Name: "ada", City: "london", Books: []string{"b1", "b2"}},
	"a2": {ID: "a2", Name: "grace", City: "new york", Books: []string{"b3"}},
	"a3": {ID: "a3", Name: "linus", City: "helsinki"},
}

var books = map[string]*book{
	"b1": {ID: "b1", Title: "notes on the engine", Year: 1843, AuthorID: "a1", Tags: []string{"maths"}},
	"b2": {ID: "b2", Title: "on looms", Year: 1845, AuthorID: "a1", Tags: []string{"maths", "craft"}},
	"b3": {ID: "b3", Title: "compiler design", Year: 1952, AuthorID: "a2", Tags: []string{"code"}},
}

func buildSchema() (graphql.Schema, error) {
	bookType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Book",
		Fields: graphql.Fields{
			"id":    &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"title": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"year":  &graphql.Field{Type: graphql.Int},
			"tags":  &graphql.Field{Type: graphql.NewList(graphql.String)},
		},
	})

	authorType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Author",
		Fields: graphql.Fields{
			"id":   &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"name": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"city": &graphql.Field{Type: graphql.String},
			"books": &graphql.Field{
				Type: graphql.NewList(bookType),
				Args: graphql.FieldConfigArgument{
					"before": &graphql.ArgumentConfig{Type: graphql.Int},
				},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					a := p.Source.(*author)
					var out []*book
					for _, id := range a.Books {
						b := books[id]
						if before, ok := p.Args["before"].(int); ok && b.Year >= before {
							continue
						}
						out = append(out, b)
					}
					return out, nil
				},
			},
			"secret": &graphql.Field{
				Type: graphql.String,
				Resolve: func(p graphql.ResolveParams) (any, error) {
					return nil, errors.New("you are not allowed to read this field")
				},
			},
		},
	})

	bookType.AddFieldConfig("author", &graphql.Field{
		Type: authorType,
		Resolve: func(p graphql.ResolveParams) (any, error) {
			return authors[p.Source.(*book).AuthorID], nil
		},
	})

	query := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"author": &graphql.Field{
				Type: authorType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					a, ok := authors[p.Args["id"].(string)]
					if !ok {
						return nil, errors.New("no author with id " + p.Args["id"].(string))
					}
					return a, nil
				},
			},
			"authors": &graphql.Field{
				Type: graphql.NewList(authorType),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					var out []*author
					for _, a := range authors {
						out = append(out, a)
					}
					slices.SortFunc(out, func(a, b *author) int { return strings.Compare(a.ID, b.ID) })
					return out, nil
				},
			},
			"search": &graphql.Field{
				Type: graphql.NewList(bookType),
				Args: graphql.FieldConfigArgument{
					"term": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					term := strings.ToLower(p.Args["term"].(string))
					var out []*book
					for _, b := range books {
						if strings.Contains(strings.ToLower(b.Title), term) {
							out = append(out, b)
						}
					}
					slices.SortFunc(out, func(a, b *book) int { return strings.Compare(a.ID, b.ID) })
					return out, nil
				},
			},
		},
	})

	mutation := graphql.NewObject(graphql.ObjectConfig{
		Name: "Mutation",
		Fields: graphql.Fields{
			"addBook": &graphql.Field{
				Type: bookType,
				Args: graphql.FieldConfigArgument{
					"title":    &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
					"year":     &graphql.ArgumentConfig{Type: graphql.Int},
					"authorId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					id := "b" + strconv.Itoa(len(books)+1)
					authorID := p.Args["authorId"].(string)
					a, ok := authors[authorID]
					if !ok {
						return nil, errors.New("no author with id " + authorID)
					}
					b := &book{ID: id, Title: p.Args["title"].(string), AuthorID: authorID}
					if y, ok := p.Args["year"].(int); ok {
						b.Year = y
					}
					books[id] = b
					a.Books = append(a.Books, id)
					return b, nil
				},
			},
		},
	})

	return graphql.NewSchema(graphql.SchemaConfig{Query: query, Mutation: mutation})
}
