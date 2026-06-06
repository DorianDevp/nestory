// Package nestory persists a Go object graph to a folder of .gob files and
// rehydrates the whole pointer graph on load. No queries, no joins — relations
// declared via struct tags become real Go pointers the moment you call [Open].
//
// Tags:
//
//	key:"primary"     int Id field (required).
//	relto:"Id"        pointer field; saved as a flattened FK, loaded as *T.
//	mapby:"UserId"    slice-of-pointer; loaded with every *T whose UserId == Id.
//
// Alpha. The API will change. Don't store anything you can't lose.
package nestory
