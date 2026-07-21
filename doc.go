// Package nestory persists a Go object graph to a folder of .gob files and
// rehydrates the whole pointer graph on load. No queries, no joins — relations
// declared via struct tags become real Go pointers the moment you call [Open].
//
// Tags:
//
//	key:"primary"          int Id field (required).
//	rel:"own,Id"           required owning pointer.
//	rel:"borrow,Id"        required non-owning pointer with delete veto.
//	rel:"option,Id"        nullable non-owning pointer.
//	rel:"inverse,User"     computed reverse collection.
//
// Alpha. The API will change. Don't store anything you can't lose.
package nestory
