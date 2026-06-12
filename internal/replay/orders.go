package replay

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type OrdersAdapter struct {
	dataPath string
}

func NewOrdersAdapter(dataPath string) *OrdersAdapter {
	return &OrdersAdapter{dataPath: dataPath}
}

func (a *OrdersAdapter) Name() string { return "orders" }

func (a *OrdersAdapter) CreateTables(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			order_id TEXT NOT NULL,
			customer_id TEXT NOT NULL,
			customer_email TEXT NOT NULL,
			created_at TEXT NOT NULL,
			currency TEXT NOT NULL,
			status TEXT NOT NULL,
			channel TEXT NOT NULL,
			shipping_state TEXT,
			subtotal_cents INTEGER NOT NULL,
			tax_cents INTEGER NOT NULL,
			total_cents INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
		CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
		CREATE INDEX IF NOT EXISTS idx_orders_channel ON orders(channel);

		CREATE TABLE IF NOT EXISTS order_items (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			order_id TEXT NOT NULL,
			sku TEXT NOT NULL,
			name TEXT NOT NULL,
			category TEXT NOT NULL,
			quantity INTEGER NOT NULL,
			unit_price_cents INTEGER NOT NULL,
			line_total_cents INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
		CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku);

		CREATE TABLE IF NOT EXISTS order_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			order_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			message TEXT NOT NULL,
			recorded_at TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events(order_id);
		CREATE INDEX IF NOT EXISTS idx_order_events_recorded_at ON order_events(recorded_at);
	`)
	return err
}

func (a *OrdersAdapter) Rows() (RowIterator, error) {
	file, err := os.Open(a.dataPath)
	if err != nil {
		return nil, fmt.Errorf("open orders data: %w", err)
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	return &ordersIterator{
		file:    file,
		scanner: scanner,
	}, nil
}

type ordersRow struct {
	OrderID       string    `json:"order_id"`
	CreatedAt     time.Time `json:"created_at"`
	CustomerID    string    `json:"customer_id"`
	CustomerEmail string    `json:"customer_email"`
	Currency      string    `json:"currency"`
	Status        string    `json:"status"`
	Channel       string    `json:"channel"`
	ShippingState string    `json:"shipping_state"`
	SubtotalCents int       `json:"subtotal_cents"`
	TaxCents      int       `json:"tax_cents"`
	TotalCents    int       `json:"total_cents"`
	Items         []struct {
		SKU            string `json:"sku"`
		Name           string `json:"name"`
		Category       string `json:"category"`
		Quantity       int    `json:"quantity"`
		UnitPriceCents int    `json:"unit_price_cents"`
	} `json:"items"`
	Events []struct {
		Type       string    `json:"type"`
		Message    string    `json:"message"`
		RecordedAt time.Time `json:"recorded_at"`
	} `json:"events"`
}

type ordersIterator struct {
	file    *os.File
	scanner *bufio.Scanner
	row     ordersRow
	err     error
}

func (it *ordersIterator) Next() bool {
	for it.scanner.Scan() {
		line := strings.TrimSpace(it.scanner.Text())
		if line == "" {
			continue
		}
		var row ordersRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			it.err = fmt.Errorf("decode orders row: %w", err)
			return false
		}
		it.row = row
		return true
	}
	it.err = it.scanner.Err()
	return false
}

func (it *ordersIterator) Timestamp() time.Time {
	return it.row.CreatedAt
}

func (it *ordersIterator) Insert(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	createdAt := it.row.CreatedAt.Format(time.RFC3339)
	if _, err := tx.Exec(`
		INSERT INTO orders (
			order_id, customer_id, customer_email, created_at, currency,
			status, channel, shipping_state, subtotal_cents, tax_cents, total_cents
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		it.row.OrderID,
		it.row.CustomerID,
		it.row.CustomerEmail,
		createdAt,
		it.row.Currency,
		it.row.Status,
		it.row.Channel,
		it.row.ShippingState,
		it.row.SubtotalCents,
		it.row.TaxCents,
		it.row.TotalCents,
	); err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	for _, item := range it.row.Items {
		lineTotal := item.Quantity * item.UnitPriceCents
		if _, err := tx.Exec(`
			INSERT INTO order_items (
				order_id, sku, name, category, quantity, unit_price_cents, line_total_cents
			) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			it.row.OrderID,
			item.SKU,
			item.Name,
			item.Category,
			item.Quantity,
			item.UnitPriceCents,
			lineTotal,
		); err != nil {
			return fmt.Errorf("insert order item: %w", err)
		}
	}

	for _, event := range it.row.Events {
		if _, err := tx.Exec(`
			INSERT INTO order_events (
				order_id, event_type, message, recorded_at
			) VALUES (?, ?, ?, ?)`,
			it.row.OrderID,
			event.Type,
			event.Message,
			event.RecordedAt.Format(time.RFC3339),
		); err != nil {
			return fmt.Errorf("insert order event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit order row: %w", err)
	}
	return nil
}

func (it *ordersIterator) Err() error { return it.err }

func (it *ordersIterator) Close() error { return it.file.Close() }
