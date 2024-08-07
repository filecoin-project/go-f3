package measurements

// Must panics if err is non-nil, otherwise returns v.
func Must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}
