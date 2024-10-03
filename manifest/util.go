package manifest

func drain[C any](ch <-chan C) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
