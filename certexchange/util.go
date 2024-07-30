package certexchange

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}
