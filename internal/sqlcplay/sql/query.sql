-- name: CreateAuthor :one
INSERT INTO authors (name, city) VALUES ($1, $2)
RETURNING *;

-- name: GetAuthor :one
SELECT * FROM authors WHERE id = $1;

-- name: AddBook :one
INSERT INTO books (author_id, title, year, tags) VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: BooksByAuthor :many
SELECT b.id, b.title, b.year, a.name AS author
FROM books b JOIN authors a ON a.id = b.author_id
WHERE a.name = $1
ORDER BY b.year;

-- name: CountBooksPerCity :many
SELECT a.city, count(*) AS books
FROM books b JOIN authors a ON a.id = b.author_id
GROUP BY a.city
ORDER BY books DESC;
