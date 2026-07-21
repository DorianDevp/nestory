package nestory

// Tx is a transaction-local view of one root entity type. Objects returned by
// Get are detached branches; all mutations are detected and committed when the
// Transaction callback returns nil.
type Tx[T Entity] struct {
	db *DB[T]
	id txId
}

// Transaction runs fn against one shared transaction context and commits every
// changed branch once. Returning an error discards the complete working set.
func (db *DB[T]) Transaction(fn func(*Tx[T]) error) error {
	id := engine.begin()
	tx := &Tx[T]{db: db, id: id}
	if err := fn(tx); err != nil {
		engine.evict(id)
		return err
	}

	if err := engine.commit(id); err != nil {
		engine.evict(id)
		return err
	}

	return nil
}

// Get loads id and its ownership subtree into this transaction. Repeated loads
// of the same node return the same transaction-local pointer.
func (tx *Tx[T]) Get(id int) (*T, error) {
	return tx.db.getInTransaction(tx.id, id)
}

// UpdateWithin mutates id inside this transaction. It joins the current context
// and does not commit independently.
func (tx *Tx[T]) UpdateWithin(id int, fn func(*T) error) error {
	entity, err := tx.Get(id)
	if err != nil {
		return err
	}

	return fn(entity)
}
