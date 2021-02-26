package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

var config Config

type Config struct {
	ImagesDir   string `json:"images_dir"`
	DsnURL      string `json:"dsn_url"`
	MaxRoutines int    `json:"max_routines"`
}

func main() {

	f, err := os.Open("config.json")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(&config)
	if err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("sqlserver", config.DsnURL)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	processDir(db)
}

func processDir(db *sql.DB) {

	filesInfo, err := ioutil.ReadDir(config.ImagesDir)
	if err != nil {
		log.Fatal(err)
	}

	if config.MaxRoutines > len(filesInfo) || config.MaxRoutines <= 0 {
		config.MaxRoutines = len(filesInfo) / 2
	}

	guard := make(chan struct{}, config.MaxRoutines)
	wg := new(sync.WaitGroup)

	for _, file := range filesInfo {

		if file.IsDir() {
			continue
		}

		guard <- struct{}{}
		go uploadImage(file.Name(), db, wg, guard)
		wg.Add(1)
	}

	fmt.Println("Waiting for all routines to finish")
	wg.Wait()
	fmt.Println("Completed")
}

func uploadImage(fileName string, db *sql.DB, wg *sync.WaitGroup, guard chan struct{}) {

	defer wg.Done()
	defer func() { <-guard }()

	fileParts := strings.Split(fileName, ".")
	if len(fileParts) < 2 {
		fmt.Println("Skipping", fileName, "as invalid")
		return
	}

	switch strings.ToUpper(fileParts[len(fileParts)-1]) {

	case "JPG", "JPEG", "PNG":

		var rimNo string
		var updateSql string
		capitalized := strings.ToUpper(fileName)

		if strings.Contains(capitalized, "_S") {
			rimNo = strings.Split(capitalized, "_S")[0]
			updateSql = "update rm_image set signature=@p1 from rm_image a,rm_acct b where a.rim_no =b.rim_no and b.old_Cust_no =@p2"
		} else {
			rimNo = strings.Split(capitalized, "_P")[0]
			updateSql = "update rm_image set photo=@p1 from rm_image a,rm_acct b where a.rim_no =b.rim_no and b.old_Cust_no =@p2"
		}

		//updateSql = "update rm_image set photo = @p1 where rim_no = (select rim_no from rm_acct where old_cust_no= @p2)"

		fmt.Println("Reading file", fileName)
		bytes, err := ioutil.ReadFile(path.Join(config.ImagesDir, fileName))
		if err != nil {
			log.Println(err)
			break
		}

		fmt.Println("Uploading image", fileName, "-->", rimNo)
		result, err := db.Exec(updateSql, bytes, rimNo)
		if err != nil {
			log.Println("Error running update script", err)
			break
		}

		rows, _ := result.RowsAffected()
		fmt.Println("Rows affected", rows, "for", rimNo)

		if rows <= 0 {
			break
		}

		filePath := path.Join(config.ImagesDir, fileName)
		err = os.Rename(filePath, path.Join(config.ImagesDir, fileName+".ok"))
		if err != nil {
			log.Println(err)
		}

	default:
		fmt.Println("Skipping file", fileName)
	}

}
