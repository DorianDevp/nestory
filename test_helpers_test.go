package nestory

import "testing"

func registerForTest[T Entity](tb testing.TB) {
	tb.Helper()
	if err := Register[T](); err != nil {
		tb.Fatal(err)
	}
}
