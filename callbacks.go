package bigquery

import (
	"github.com/basemachina/go-bigquery/adaptor"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
)

func initializeCallbacks(db *gorm.DB) {

	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES", "ON CONFLICT", "RETURNING"},
	})

	c := &bigQueryCallbacks{db}

	queryCallback := db.Callback().Query()
	queryCallback.Replace("gorm:query", c.queryCallback)
}

type bigQueryCallbacks struct {
	root *gorm.DB
}

func (c *bigQueryCallbacks) queryCallback(db *gorm.DB) {
	if !db.DryRun {
		applyStatementSchemaContext(db, c.root)
	}

	callbacks.Query(db)
}

func applyStatementSchemaContext(db *gorm.DB, rootDB *gorm.DB) {
	db.Statement.Context = adaptor.SetSchemaAdaptor(db.Statement.Context, &bigQuerySchemaAdaptor{
		db.Statement.Schema,
		rootDB,
	})
}
