package talon

import (
	"fmt"
	"os"
	"testing"
)

// Copied into the pinned SDK checkout by the release workflow. Every Open
// verifies the signed artifact before touching this private temporary database.
func TestReleaseCountAfter45RowsReopenAnd14More(t *testing.T) {
	if os.Getenv("TALON_TEST_PRODUCTION_NATIVE") != "1" && os.Getenv("TALON_TEST_EMBEDDED_NATIVE") != "1" {
		t.Fatal("count/reopen release gate requires a signed native or embedded runtime")
	}
	path := t.TempDir()
	var db *DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})
	open := func() {
		t.Helper()
		var err error
		db, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		info := db.NativeInfo()
		if info.Admission != NativeAdmissionRelease || info.CoreGitDirty {
			t.Fatalf("release gate loaded an untrusted or dirty runtime: %+v", info)
		}
		t.Logf("loaded release identity: %+v", info)
	}
	exec := func(sql string, args ...Value) {
		t.Helper()
		if err := db.Exec(sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	insert := func(first, last int) {
		t.Helper()
		for version := first; version <= last; version++ {
			name, err := TextValue(fmt.Sprintf("schema_%d", version))
			if err != nil {
				t.Fatal(err)
			}
			exec("INSERT INTO release_count_probe VALUES (?,?,?,?)", name, IntegerValue(int64(version)), IntegerValue(1700000000), IntegerValue(1))
		}
	}
	counts := func(want int64) {
		t.Helper()
		rows, err := db.Query("SELECT * FROM release_count_probe ORDER BY schema_name,version LIMIT 20001")
		if err != nil || int64(len(rows)) != want {
			t.Fatalf("bounded scan: rows=%d want=%d err=%v", len(rows), want, err)
		}
		for _, expression := range []string{"COUNT(*)", "COUNT(version)", "COUNT(schema_name)"} {
			for _, alias := range []string{"", " AS total"} {
				sql := "SELECT " + expression + alias + " FROM release_count_probe"
				rows, err := db.Query(sql)
				if err != nil || len(rows) != 1 || len(rows[0]) != 1 || rows[0].Int(0) != want {
					t.Fatalf("%s: rows=%#v want=%d err=%v", sql, rows, want, err)
				}
			}
		}
		rows, err = db.Query("SELECT COUNT(*) AS total FROM release_count_probe WHERE version > ?", IntegerValue(0))
		if err != nil || len(rows) != 1 || rows[0].Int(0) != want {
			t.Fatalf("filtered count: rows=%#v want=%d err=%v", rows, want, err)
		}
	}
	open()
	exec("CREATE TABLE release_count_probe (schema_name TEXT NOT NULL, version INTEGER NOT NULL, applied_at INTEGER NOT NULL, statement_count INTEGER NOT NULL, PRIMARY KEY (schema_name,version))")
	insert(1, 45)
	counts(45)
	db.Close()
	open()
	counts(45)
	insert(46, 59)
	counts(59)
	counts(59)
	rows, err := db.Query("SELECT COUNT(*),COUNT(version),SUM(version) FROM release_count_probe")
	if err != nil || len(rows) != 1 || len(rows[0]) != 3 || rows[0].Int(0) != 59 || rows[0].Int(1) != 59 || rows[0].Int(2) != 1770 {
		t.Fatalf("mixed aggregate: rows=%#v err=%v", rows, err)
	}
	exec("BEGIN")
	counts(59)
	insert(60, 60)
	counts(60)
	exec("DELETE FROM release_count_probe WHERE version=1")
	counts(59)
	exec("ROLLBACK")
	counts(59)
	db.Close()
	open()
	counts(59)
}
