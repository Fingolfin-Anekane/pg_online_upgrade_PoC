package oracle

import (
	"context"
	"testing"

	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func expectAggregates(mock pgxmock.PgxPoolIface, dupID int64, lastVal, maxID, sum int64, accounts int64) {
	mock.ExpectQuery("FROM .*HAVING count").
		WillReturnRows(pgxmock.NewRows([]string{"c"}).AddRow(dupID))
	mock.ExpectQuery("last_value FROM events_id_seq").
		WillReturnRows(pgxmock.NewRows([]string{"last_value"}).AddRow(lastVal))
	mock.ExpectQuery("coalesce\\(max\\(id\\),0\\)").
		WillReturnRows(pgxmock.NewRows([]string{"m"}).AddRow(maxID))
	mock.ExpectQuery("FROM accounts").
		WillReturnRows(pgxmock.NewRows([]string{"sum", "cnt"}).AddRow(sum, accounts))
}

func TestReconcileDetectsLost(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// membership: writer 7 seq 5 present once; seq 6 absent.
	mock.ExpectQuery("GROUP BY writer_id, client_seq").
		WillReturnRows(pgxmock.NewRows([]string{"writer_id", "client_seq", "c"}).
			AddRow(7, int64(5), int64(1)))
	expectAggregates(mock, 0, 6, 5, 100000, 100)

	ops := []Op{
		{OpID: "o1", WriterID: 7, ClientSeq: 5, Rows: 1, Status: "acked"},
		{OpID: "o2", WriterID: 7, ClientSeq: 6, Rows: 1, Status: "acked"}, // LOST
	}
	f, err := Reconcile(context.Background(), mock, ops, 1000)
	require.NoError(t, err)
	assert.Len(t, f.Lost, 1)
	assert.Equal(t, "o2", f.Lost[0])
	assert.Empty(t, f.Dup)
	assert.Empty(t, f.Phantom)
	assert.True(t, f.Failed()) // a LOST finding must fail the run
}

func TestReconcileDetectsPhantomAndSeq(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("GROUP BY writer_id, client_seq").
		WillReturnRows(pgxmock.NewRows([]string{"writer_id", "client_seq", "c"}).
			AddRow(7, int64(5), int64(1)))
	// dup id = 2, last_value 4 < max id 5 -> seq behind, sum mismatch
	expectAggregates(mock, 2, 4, 5, 99000, 100)

	ops := []Op{{OpID: "f1", WriterID: 7, ClientSeq: 5, Rows: 1, Status: "failed"}} // present -> phantom
	f, err := Reconcile(context.Background(), mock, ops, 1000)
	require.NoError(t, err)
	assert.Equal(t, []string{"f1"}, f.Phantom)
	assert.Equal(t, int64(2), f.DupID)
	assert.True(t, f.SeqBehind)
	assert.True(t, f.Failed())
}

func TestReconcileCleanPass(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("GROUP BY writer_id, client_seq").
		WillReturnRows(pgxmock.NewRows([]string{"writer_id", "client_seq", "c"}).
			AddRow(7, int64(5), int64(1)).
			AddRow(7, int64(6), int64(1)))
	expectAggregates(mock, 0, 6, 6, 100000, 100) // last_value==max, sum matches

	ops := []Op{
		{OpID: "o1", WriterID: 7, ClientSeq: 5, Rows: 1, Status: "acked"},
		{OpID: "o2", WriterID: 7, ClientSeq: 6, Rows: 1, Status: "acked"},
	}
	f, err := Reconcile(context.Background(), mock, ops, 1000)
	require.NoError(t, err)
	assert.False(t, f.Failed())
	assert.Empty(t, f.Lost)
	assert.Empty(t, f.Dup)
	assert.Empty(t, f.Phantom)
}

func TestReconcileDupBatchAppendsOnce(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// long-txn op rows=3 at seq 10,11,12; seq 10 and 11 are duplicated (count 2)
	mock.ExpectQuery("GROUP BY writer_id, client_seq").
		WillReturnRows(pgxmock.NewRows([]string{"writer_id", "client_seq", "c"}).
			AddRow(3, int64(10), int64(2)).
			AddRow(3, int64(11), int64(2)).
			AddRow(3, int64(12), int64(1)))
	expectAggregates(mock, 0, 12, 12, 100000, 100)

	ops := []Op{{OpID: "b1", WriterID: 3, ClientSeq: 10, Rows: 3, Status: "acked"}}
	f, err := Reconcile(context.Background(), mock, ops, 1000)
	require.NoError(t, err)
	assert.Equal(t, []string{"b1"}, f.Dup) // once, despite two duplicated rows
	assert.Empty(t, f.Lost)                // all three rows present
}
