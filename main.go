package main

import (
	"database/sql"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"strings"
	"time"
)

type Lecturer struct {
	id   int64
	name string
}

func normalizeLecturers(db *sql.DB) []Lecturer {
	rows, err := db.Query("SELECT lecturer FROM courses WHERE lecturer != ''")
	if err != nil {
		log.Fatal(err)
	}

	lecturers := make(map[string]bool)

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatal(err)
		}

		names := strings.Split(name, ",")
		for _, n := range names {
			lecturers[strings.TrimSpace(n)] = true
		}
	}

	result := make([]Lecturer, 0)

	for k, _ := range lecturers {
		result = append(result, Lecturer{name: k})
	}

	return result
}

func saveLecturers(db *sql.DB, lecturers []Lecturer) []Lecturer {
	result := make([]Lecturer, 0)

	const query = "INSERT INTO lecturers(name) VALUES($1) RETURNING ID"

	for _, lecturer := range lecturers {
		var id int64

		row := db.QueryRow(query, lecturer.name)
		if err := row.Scan(&id); err != nil {
			log.Fatal(err)
		}

		result = append(result, Lecturer{id:id, name:lecturer.name})
	}

	return result
}

func getCourses(db *sql.DB) []Course {
	result := make([]Course, 0)

	const query = "SELECT rootNumber, sn, title FROM courses"

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var course Course
		if err := rows.Scan(&course.rootNumber, &course.sn, &course.title); err != nil {
			log.Fatal(err)
		}

		result = append(result, course)
	}

	return result
}

func importCourses(db *sql.DB, courses []Course) []Course {
	const query = "INSERT INTO courses(rootNumber, sn, title) VALUES($1, $2, $3) RETURNING id"
	result := make([]Course, 0)

	for _, c := range courses {
		course := Course{sn:c.sn, title:c.title, rootNumber:c.rootNumber}
		row := db.QueryRow(query, c.rootNumber, c.sn, c.title)

		if err := row.Scan(&course.id); err != nil {
			log.Fatal(err)
		}

		result = append(result, course)
	}

	return result
}

func initDatabase() *sql.DB {
	const initSQL = `
	
	DROP TABLE IF EXISTS lecturers;
	DROP TABLE IF EXISTS coursedate;
	DROP TABLE IF EXISTS courses;

	CREATE TABLE IF NOT EXISTS lecturers(
		id BIGSERIAL PRIMARY KEY NOT NULL,
		name TEXT NOT NULL UNIQUE
	);

	CREATE TABLE IF NOT EXISTS courses(
		id BIGSERIAL PRIMARY KEY NOT NULL,
		rootNumber INTEGER NOT NULL,
		sn INTEGER NOT NULL,
		title TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS coursedate(
		id BIGSERIAL PRIMARY KEY NOT NULL,
		courseId BIGINT REFERENCES courses NOT NULL,
		start timestamp NOT NULL,
		"end" timestamp NOT NULL,
		assessment BOOLEAN NOT NULL	
	);
`

	db, err := sql.Open("postgres", "postgres://postgres:@localhost/?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(initSQL)
	if err != nil {
		db.Close()
		log.Fatal(err)
	}

	return db
}

type Course struct {
	id int64
	rootNumber int32
	sn int
	title string
}

type CourseDate struct {
	id int64
	courseId int64
	from time.Time
	to time.Time
	assessment bool
}

const (
	ColWeekday = iota + 1
	ColDate
	ColBeg
	ColEnd
	ColAssesment
)

func getTimeSchedule(db *sql.DB, courses []Course)[]CourseDate{
	result := make([]CourseDate, 0)

	const query = "SELECT rawDetail FROM courses WHERE rootNumber = $1 AND sn = $2"

	for _, c := range courses {
		var src string
		row := db.QueryRow(query, c.rootNumber, c.sn)
		if err := row.Scan(&src); err != nil {
			log.Fatal(err)
		}

		doc, err := goquery.NewDocumentFromReader(strings.NewReader(src))
		if err != nil {
			log.Fatal(err)
		}

		tables := doc.Find("table").FilterFunction(func(i int, selection *goquery.Selection) bool {
			_, hasBorder := selection.Attr("border")
			cols := len(selection.Find("colgroup").First().Find("col").Nodes)
			return hasBorder && cols == 9
		})

		table := tables.First()

		trs := table.Find("tr")

		for _, tr := range trs.Nodes {
			tds := doc.FindNodes(tr).Find("td").Nodes

			if (len(tds) == 0) {
				continue
			}

			beginning, err := time.Parse("2/1/2006 15:04",
				doc.FindNodes(tds[ColDate]).Text()+" "+
				doc.FindNodes(tds[ColBeg]).Text())

			if err != nil {
				log.Fatal(err)
			}

			end, err := time.Parse("2/1/2006 15:04",
				doc.FindNodes(tds[ColDate]).Text()+" "+
					doc.FindNodes(tds[ColEnd]).Text())

			if err != nil {
				log.Fatal(err)
			}

			assessment := doc.FindNodes(tds[ColAssesment]).Text() != "No"

			courseDate := CourseDate{
				courseId:c.id,
				assessment:assessment,
				from:beginning,
				to:end,
			}

			result = append(result, courseDate)
		}
	}

	return result
}

func importTimeSchedule(db *sql.DB, entries []CourseDate) {
	const query = "INSERT INTO coursedate(courseId, start, \"end\", assessment) VALUES ($1, $2, $3, $4)"

	for _, c := range entries {
		_, err := db.Exec(query, c.courseId, c.from, c.to, c.assessment)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	src, err := sql.Open("sqlite3", "./data.db")
	defer src.Close()

	if err != nil {
		log.Fatal(err)
	}

	lecturers := normalizeLecturers(src)

	// insert
	postgres := initDatabase()

	_ = saveLecturers(postgres, lecturers)

	courses := getCourses(src)

	courses = importCourses(postgres, courses)


	courseDates := getTimeSchedule(src, courses)
	fmt.Println(len(courseDates))

	importTimeSchedule(postgres, courseDates)

	defer postgres.Close()
}
