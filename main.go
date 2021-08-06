package main

import (
  "database/sql"
  "log"
  "net/http"
  "fmt"
  "strings"
  "sync"
  "time"
  "github.com/gorilla/mux"
  _ "github.com/go-sql-driver/mysql"
)

func dbConn() (db *sql.DB) {
  dbhost := "test.db"
  dbport := "3306"
  dbDriver := "mysql"
  dbUser := "user"
  dbPass := "pass"
  dbName := "test"
  db, err := sql.Open(dbDriver, dbUser + ":" + dbPass + "@tcp(" + dbhost + ":" + dbport + ")/" + dbName)
  if err != nil {
    panic(err.Error())
  }
  return db
}

func Delete(w http.ResponseWriter, r *http.Request) {
  log.Println("Begin delete at " + time.Now().String())
  db := dbConn()
  //emp := r.URL.Query().Get("id")
  idList := make([]interface{}, 520000)
  for i:= range idList {
    idList[i] = i
  }
  sql := "DELETE FROM authors where id IN (?" + strings.Repeat(",?", 999) + ")"
  log.Println("SQL: " + sql)
  stmt, err := db.Prepare(sql)
  if err != nil {
    panic(err.Error())
  }
  var wg sync.WaitGroup
  wg.Add(len(idList)/1000)
  var j int
  var size = 1000
  for i := 0; i < len(idList); i += size {
    j += size
    if j > len(idList) {
        j = len(idList)
    }
    go execDelete(&wg, stmt, idList[i:j])
  }
  wg.Wait()
  defer db.Close()
  log.Println("Finished delete at " + time.Now().String())
  fmt.Fprintf(w, "Row deleted!")
}

func execDelete(wg *sync.WaitGroup, stmt *sql.Stmt, id []interface{}) {
  defer wg.Done()
  _, err := stmt.Exec(id...)
  if err != nil {
    panic(err.Error())
  }
}

func main() {
  log.Println("Server started on: http://localhost:8080")
  r := mux.NewRouter()
  r.HandleFunc("/delete", Delete).Methods("GET")
  log.Fatal(http.ListenAndServe(":8080", r))
}
