package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/search/query"
)

type Record struct {
	InventoryID int64       `json:"inventory_id"`
	Geog        interface{} `json:"geog"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "not enough arguments\n")
		os.Exit(1)
	}

	switch strings.ToLower(os.Args[1]) {
	case "q", "qry", "query":
		index, _ := bleve.Open("index.bleve")
		defer index.Close()
		geojson := `{"type":"Polygon","coordinates":[[[-152,62],[-152,59],[-147,59],[-147,62],[-152,62]]]}` // time 500 ms, hits: 51,092
		// geojson := `{"type":"Polygon","coordinates":[[[-152,62],[-152,59],[-151,59],[-151,62],[-152,62]]]}` // time:486 ms, hits: 35,813
		// geojson := `{"type":"Polygon","coordinates":[[[-152,62],[-152,61],[-151,61],[-151,62],[-152,62]]]}`  // time: 515 ms, hits: 12,474
		// geojson := `{"type":"Polygon","coordinates":[[[-152,62],[-152,61],[-151.9,61],[-151.9,62],[-152,62]]]}` // time: 501 ms, hits: 363
		// geojson := `{"type":"Polygon","coordinates":[[[-152,62],[-152,61.9],[-151.9,61.9],[-151.9,62],[-152,62]]]}` // time: 373, hits: 6

		shape, _ := geo.ParseGeoJSONShape([]byte(geojson))
		gsq := &query.GeoShapeQuery{
			FieldVal: "geog",
			Geometry: query.Geometry{
				Shape:    shape,
				Relation: "within",
			},
		}
		sreq := bleve.NewSearchRequest(gsq)
		sreq.Size = 10
		sreq.Fields = []string{"geojson"}

		sres, _ := index.Search(sreq)
		fmt.Printf("Took: %s\n", sres.Took.String())
		fmt.Printf("Total hits: %d\n", sres.Total)

	case "i", "idx", "index":
		index, err := bleve.Open("index.bleve")
		if err == bleve.ErrorIndexPathDoesNotExist {
			dmap := bleve.NewDocumentStaticMapping()
			gg_f := bleve.NewGeoShapeFieldMapping()
			dmap.AddFieldMappingsAt("geog", gg_f)

			imap := bleve.NewIndexMapping()
			imap.DefaultMapping = dmap

			index, _ = bleve.New("index.bleve", imap)
		}
		defer index.Close()

		file, err := os.Open("output.brotli")
		if err != nil {
			panic(err)
		}
		defer file.Close()

		brotliReader := brotli.NewReader(file)

		var records []Record
		err = json.NewDecoder(brotliReader).Decode(&records)
		if err != nil {
			panic(err)
		}

		batch := index.NewBatch()

		for _, record := range records {
			ref := make(map[string]interface{}, 1)
			ref["geog"] = record.Geog

			batch.Index(strconv.FormatInt(record.InventoryID, 10), ref)
			if batch.Size() > 1000 {
				index.Batch(batch)
				fmt.Print(".")
				batch.Reset()
			}
		}
		if batch.Size() > 0 {
			index.Batch(batch)
		}
		fmt.Println()
	}
}
