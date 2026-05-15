// Package nestory persists Go object graphs to a folder of .gob files and
// rehydrates the entire graph of pointers on load. There are no queries,
// no joins — relations declared via struct tags become real Go pointers
// the moment you call [Open].
//
// # Tags
//
//	key:"primary"     marks the int Id field of an entity (required).
//	relto:"Id"        pointer field; on save, flattened to a foreign key
//	                  column; on load, rehydrated to *T pointing at the
//	                  matching instance in the related base.
//	mapby:"UserId"    slice-of-pointer field; on load, populated with every
//	                  *T whose UserId field equals this entity's Id.
//
// # Status
//
// Alpha. The API surface will change before 1.0. Don't use for data you
// can't afford to lose.
package nestory
