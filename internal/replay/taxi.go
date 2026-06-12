package replay

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

type TaxiAdapter struct {
	dataPath string
}

func NewTaxiAdapter(dataPath string) *TaxiAdapter {
	return &TaxiAdapter{dataPath: dataPath}
}

func (a *TaxiAdapter) Name() string { return "taxi" }

func (a *TaxiAdapter) CreateTables(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS taxi_trips (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			vendor_id INTEGER,
			pickup_datetime TEXT NOT NULL,
			dropoff_datetime TEXT NOT NULL,
			passenger_count INTEGER,
			trip_distance REAL,
			pickup_location_id INTEGER,
			dropoff_location_id INTEGER,
			rate_code_id INTEGER,
			payment_type INTEGER,
			fare_amount REAL,
			extra REAL,
			mta_tax REAL,
			tip_amount REAL,
			tolls_amount REAL,
			total_amount REAL,
			congestion_surcharge REAL,
			airport_fee REAL
		);
		CREATE INDEX IF NOT EXISTS idx_taxi_pickup ON taxi_trips(pickup_datetime);
	`)
	return err
}

func (a *TaxiAdapter) Rows() (RowIterator, error) {
	f, err := os.Open(a.dataPath)
	if err != nil {
		return nil, fmt.Errorf("open taxi data: %w", err)
	}

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}

	colIndex := make(map[string]int)
	for i, col := range header {
		colIndex[col] = i
	}

	return &taxiIterator{
		file:     f,
		reader:   r,
		colIndex: colIndex,
	}, nil
}

type taxiIterator struct {
	file     *os.File
	reader   *csv.Reader
	colIndex map[string]int
	row      []string
	err      error
}

func (it *taxiIterator) Next() bool {
	it.row, it.err = it.reader.Read()
	if it.err == io.EOF {
		it.err = nil
		return false
	}
	return it.err == nil
}

func (it *taxiIterator) Timestamp() time.Time {
	col := "tpep_pickup_datetime"
	idx, ok := it.colIndex[col]
	if !ok || idx >= len(it.row) {
		return time.Time{}
	}
	t, _ := time.Parse("2006-01-02 15:04:05", it.row[idx])
	return t
}

func (it *taxiIterator) Insert(db *sql.DB) error {
	get := func(col string) string {
		idx, ok := it.colIndex[col]
		if !ok || idx >= len(it.row) {
			return ""
		}
		return it.row[idx]
	}
	getFloat := func(col string) float64 {
		v, _ := strconv.ParseFloat(get(col), 64)
		return v
	}
	getInt := func(col string) int {
		v, _ := strconv.Atoi(get(col))
		return v
	}

	_, err := db.Exec(`
		INSERT INTO taxi_trips (
			vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
			trip_distance, pickup_location_id, dropoff_location_id,
			rate_code_id, payment_type, fare_amount, extra, mta_tax,
			tip_amount, tolls_amount, total_amount, congestion_surcharge, airport_fee
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		getInt("VendorID"),
		get("tpep_pickup_datetime"),
		get("tpep_dropoff_datetime"),
		getInt("passenger_count"),
		getFloat("trip_distance"),
		getInt("PULocationID"),
		getInt("DOLocationID"),
		getInt("RatecodeID"),
		getInt("payment_type"),
		getFloat("fare_amount"),
		getFloat("extra"),
		getFloat("mta_tax"),
		getFloat("tip_amount"),
		getFloat("tolls_amount"),
		getFloat("total_amount"),
		getFloat("congestion_surcharge"),
		getFloat("Airport_fee"),
	)
	return err
}

func (it *taxiIterator) Err() error { return it.err }

func (it *taxiIterator) Close() error { return it.file.Close() }
